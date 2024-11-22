from __future__ import annotations

import asyncio
import logging
import typing as t
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from aiormq import Channel, Connection, ProtocolSyntaxError, spec
from pamqp.common import FieldTable

from brokrpc.retry import create_delay_retryer
from brokrpc.stringify import to_str_obj

if t.TYPE_CHECKING:
    from aiormq.abc import DeliveredMessage

from brokrpc.abc import BinaryConsumer, BinaryPublisher, BoundConsumer, BrokerDriver, Publisher
from brokrpc.message import BinaryMessage, Message
from brokrpc.model import (
    ConsumerAck,
    ConsumerReject,
    ConsumerResult,
    ConsumerRetry,
    PublisherResult,
)
from brokrpc.options import (
    BindingOptions,
    BrokerOptions,
    ExchangeOptions,
    PublisherOptions,
    QOSOptions,
    QueueOptions,
)


class AiormqMessage(Message[bytes]):
    __slots__ = ("__impl",)

    def __init__(self, impl: DeliveredMessage) -> None:
        self.__impl = impl
        assert isinstance(self.__impl.routing_key, str)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__impl!r})"

    @property
    def body(self) -> bytes:
        return self.__impl.body

    @property
    def routing_key(self) -> str:
        assert isinstance(self.__impl.routing_key, str)
        return self.__impl.routing_key

    @property
    def exchange(self) -> str | None:
        return self.__impl.exchange

    @property
    def content_type(self) -> str | None:
        return self.__impl.header.properties.content_type

    @property
    def content_encoding(self) -> str | None:
        return self.__impl.header.properties.content_encoding

    @property
    def headers(self) -> t.Mapping[str, str] | None:
        return t.cast(t.Mapping[str, str], self.__impl.header.properties.headers)

    @property
    def delivery_mode(self) -> int | None:
        return self.__impl.header.properties.delivery_mode

    @property
    def priority(self) -> int | None:
        return self.__impl.header.properties.priority

    @property
    def correlation_id(self) -> str | None:
        return self.__impl.header.properties.correlation_id

    @property
    def reply_to(self) -> str | None:
        return self.__impl.header.properties.reply_to

    @property
    def timeout(self) -> timedelta | None:
        return _load_message_timeout(self.__impl.header.properties.expiration)

    @property
    def message_id(self) -> str | None:
        return self.__impl.header.properties.message_id

    @property
    def timestamp(self) -> datetime | None:
        return self.__impl.header.properties.timestamp

    @property
    def message_type(self) -> str | None:
        return self.__impl.header.properties.message_type

    @property
    def user_id(self) -> str | None:
        return self.__impl.header.properties.user_id

    @property
    def app_id(self) -> str | None:
        return self.__impl.header.properties.app_id


class AiormqPublisher(Publisher[BinaryMessage, PublisherResult]):
    def __init__(
        self,
        channel: Channel,
        options: PublisherOptions | None,
        message_id_gen: t.Callable[[], str | None],
        now: t.Callable[[], datetime | None],
    ) -> None:
        self.__channel = channel
        self.__options = options
        self.__message_id_gen = message_id_gen
        self.__now = now

    async def publish(self, message: BinaryMessage) -> PublisherResult:
        await self.__channel.basic_publish(
            body=message.body,
            exchange=message.exchange
            if message.exchange is not None
            else self.__options.name
            if self.__options is not None and self.__options.name is not None
            else "",
            routing_key=message.routing_key,
            mandatory=self.__options.mandatory
            if self.__options is not None and self.__options.mandatory is not None
            else False,
            properties=spec.Basic.Properties(
                content_type=message.content_type,
                content_encoding=message.content_encoding,
                headers=t.cast(FieldTable | None, message.headers),
                delivery_mode=message.delivery_mode,
                priority=message.priority,
                correlation_id=message.correlation_id,
                reply_to=message.reply_to,
                expiration=_dump_message_timeout(message.timeout),
                message_id=message.message_id if message.message_id is not None else self.__message_id_gen(),
                timestamp=message.timestamp if message.timestamp is not None else self.__now(),
                message_type=message.message_type,
                user_id=message.user_id,
                app_id=message.app_id,
            ),
        )

        return None


class AiormqConsumerCallback:
    def __init__(self, channel: Channel, inner: BinaryConsumer) -> None:
        self.__channel = channel
        self.__inner = inner

    async def __call__(self, aiormq_message: DeliveredMessage) -> None:
        assert isinstance(aiormq_message.delivery_tag, int)
        assert isinstance(aiormq_message.routing_key, str)

        message = AiormqMessage(aiormq_message)
        result = await self.__inner.consume(message)
        # TODO: check how aiormq handles exception
        await self.__handle_result(aiormq_message, result)

    async def __handle_result(self, message: DeliveredMessage, result: ConsumerResult) -> None:
        assert isinstance(message.delivery_tag, int)

        match result:
            case ConsumerAck() | True | None:
                await self.__channel.basic_ack(message.delivery_tag)

            case ConsumerReject() | False:
                await self.__channel.basic_reject(message.delivery_tag, requeue=False)

            case ConsumerRetry(delay=delay):
                if delay is not None:
                    await asyncio.sleep(delay.total_seconds())

                # TODO: AMQP returns message to the head of the queue, think about option to return it to the end of the
                #  queue (real requeue).
                await self.__channel.basic_reject(message.delivery_tag)

            case _:
                t.assert_never(result)


class AiormqBoundConsumer(BoundConsumer):
    def __init__(
        self,
        channel: Channel,
        callback: AiormqConsumerCallback,
        options: BindingOptions,
        consumer_tag: str,
    ) -> None:
        self.__channel = channel
        self.__callback = callback
        self.__options = options
        self.__consumer_tag = consumer_tag

    def is_alive(self) -> bool:
        return self.__consumer_tag in self.__channel.consumers

    def get_options(self) -> BindingOptions:
        return self.__options


class AiormqBrokerDriver(BrokerDriver):
    @classmethod
    @asynccontextmanager
    async def connect(cls, options: BrokerOptions) -> t.AsyncIterator[AiormqBrokerDriver]:
        def log_warning(err: Exception) -> None:
            logging.warning("can't connect to rabbitmq", exc_info=err)

        retryer = create_delay_retryer(
            options=options,
            errors=(ConnectionError, ProtocolSyntaxError),
            on_attempt_error=log_warning,
        )

        conn = Connection(options.url)
        try:
            await retryer.do(conn.connect)

            yield cls(conn)

        finally:
            await conn.close()

    def __init__(self, connection: Connection) -> None:
        self.__connection = connection

    def __str__(self) -> str:
        return to_str_obj(self, connection=self.__connection)

    @asynccontextmanager
    async def provide_publisher(self, options: PublisherOptions | None = None) -> t.AsyncIterator[BinaryPublisher]:
        async with self.__provide_channel(options) as channel:
            yield AiormqPublisher(channel, options, self.__gen_message_id, self.__get_now)

    @asynccontextmanager
    async def bind_consumer(self, consumer: BinaryConsumer, options: BindingOptions) -> t.AsyncIterator[BoundConsumer]:
        channel: Channel

        async with self.__provide_channel(options.queue) as channel:
            callback = AiormqConsumerCallback(channel, consumer)

            bindings = await self.__bind(channel, options)
            assert isinstance(bindings.queue, QueueOptions)
            assert isinstance(bindings.queue.name, str)

            consume_ok = await channel.basic_consume(
                queue=bindings.queue.name,
                consumer_callback=callback,
            )
            assert isinstance(consume_ok.consumer_tag, str)

            try:
                yield AiormqBoundConsumer(channel, callback, bindings, consume_ok.consumer_tag)

            finally:
                await channel.basic_cancel(consume_ok.consumer_tag)

    @asynccontextmanager
    async def __provide_channel(self, options: QOSOptions | None) -> t.AsyncIterator[Channel]:
        channel = await self.__connection.channel()
        assert isinstance(channel, Channel)

        try:
            if options is not None:
                await channel.basic_qos(
                    prefetch_count=options.prefetch_count,
                )

            yield channel

        finally:
            await channel.close()

    async def __setup_exchange(
        self,
        channel: Channel,
        options: ExchangeOptions | None,
    ) -> tuple[str, ExchangeOptions]:
        name = options.name if options is not None and options.name is not None else ""
        type_ = options.type if options is not None and options.type is not None else "direct"
        durable = options.durable if options is not None and options.durable is not None else False
        auto_delete = options.auto_delete if options is not None and options.auto_delete is not None else False
        arguments = options.arguments if options is not None and options.arguments is not None else None

        await channel.exchange_declare(
            exchange=name,
            exchange_type=type_,
            durable=durable,
            auto_delete=auto_delete,
            # TODO: remove ignore
            arguments=arguments,  # type: ignore[arg-type]
        )

        return name, ExchangeOptions(
            name=name,
            type=type_,
            durable=durable,
            auto_delete=auto_delete,
            arguments=arguments,
        )

    async def __setup_queue(
        self,
        channel: Channel,
        options: QueueOptions | None,
    ) -> tuple[str, QueueOptions]:
        name = options.name if options is not None and options.name is not None else ""
        durable = options.durable if options is not None and options.durable is not None else False
        exclusive = options.exclusive if options is not None and options.exclusive is not None else False
        auto_delete = options.auto_delete if options is not None and options.auto_delete is not None else False
        arguments = options.arguments if options is not None and options.arguments is not None else None

        declare_ok = await channel.queue_declare(
            queue=name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            # TODO: remove ignore
            arguments=arguments,  # type: ignore[arg-type]
        )
        assert isinstance(declare_ok.queue, str)

        return declare_ok.queue, QueueOptions(
            name=declare_ok.queue,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
        )

    async def __bind(self, channel: Channel, options: BindingOptions) -> BindingOptions:
        exchange_name, exchange_options = await self.__setup_exchange(channel, options.exchange)
        queue_name, queue_options = await self.__setup_queue(channel, options.queue)

        await asyncio.gather(
            *(
                channel.queue_bind(
                    exchange=exchange_name,
                    routing_key=binding_key,
                    queue=queue_name,
                )
                for binding_key in options.binding_keys
            )
        )

        return replace(options, exchange=exchange_options, queue=queue_options)

    def __gen_message_id(self) -> str:
        return uuid4().hex

    def __get_now(self) -> datetime:
        return datetime.now(tz=UTC)


def _load_message_timeout(value: str | None) -> timedelta | None:
    return timedelta(milliseconds=float(value)) if value else None


def _dump_message_timeout(value: timedelta | None) -> str | None:
    return f"{value.total_seconds() * 1000:.0f}" if value is not None else None
