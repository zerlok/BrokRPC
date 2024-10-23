from __future__ import annotations

import asyncio
import logging
import typing as t
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from aiormq import Channel, Connection, ProtocolSyntaxError, spec
from pamqp.common import FieldTable

if t.TYPE_CHECKING:
    from aiormq.abc import DeliveredMessage
    from yarl import URL

from protomq.abc import BoundConsumer, Consumer, Driver, Publisher
from protomq.message import ConsumerAck, ConsumerReject, ConsumerResult, ConsumerRetry, Message
from protomq.options import (
    BindingOptions,
    ChannelOptions,
    ExchangeOptions,
    PublisherOptions,
    QueueOptions,
)


class AiormqPublisher(Publisher):
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

    async def publish(self, message: Message) -> None:
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
                expiration=message.expiration,
                message_id=self.__message_id_gen(),
                timestamp=self.__now(),
                message_type=message.message_type,
                user_id=message.user_id,
                app_id=message.app_id,
            ),
        )


class AiormqConsumerCallback:
    def __init__(self, channel: Channel, inner: Consumer) -> None:
        self.__channel = channel
        self.__inner = inner

    async def __call__(self, aiormq_message: DeliveredMessage) -> None:
        assert isinstance(aiormq_message.delivery_tag, int)

        try:
            message = self.__build_message(aiormq_message)
            result = await self.__inner.consume(message)

        except Exception as err:
            logging.exception("fatal consumer err", exc_info=err)
            await self.__channel.basic_reject(
                delivery_tag=aiormq_message.delivery_tag,
                requeue=False,
            )

        else:
            await self.__handle_result(aiormq_message, result)

    def __build_message(self, message: DeliveredMessage) -> Message:
        assert isinstance(message.routing_key, str)

        return Message(
            body=message.body,
            exchange=message.exchange,
            routing_key=message.routing_key,
            content_type=message.header.properties.content_type,
            content_encoding=message.header.properties.content_encoding,
            headers=t.cast(t.Mapping[str, str], message.header.properties.headers),
            delivery_mode=message.header.properties.delivery_mode,
            priority=message.header.properties.priority,
            correlation_id=message.header.properties.correlation_id,
            reply_to=message.header.properties.reply_to,
            expiration=message.header.properties.expiration,
            message_id=message.header.properties.message_id,
            timestamp=message.header.properties.timestamp,
            message_type=message.header.properties.message_type,
            user_id=message.header.properties.user_id,
            app_id=message.header.properties.app_id,
        )

    async def __handle_result(self, message: DeliveredMessage, result: ConsumerResult) -> None:
        assert isinstance(message.delivery_tag, int)

        match result:
            case ConsumerAck():
                await self.__channel.basic_ack(message.delivery_tag)

            case ConsumerReject():
                await self.__channel.basic_nack(message.delivery_tag, requeue=False)

            case ConsumerRetry(delay=delay):
                if delay is not None:
                    await asyncio.sleep(delay.total_seconds())
                await self.__channel.basic_nack(message.delivery_tag)


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


class AiormqDriver(Driver):
    @classmethod
    @asynccontextmanager
    async def connect(
        cls,
        url: URL,
        max_attempts: int = 10,
        attempt_delay: timedelta = timedelta(seconds=3.0),
    ) -> t.AsyncIterator[AiormqDriver]:
        conn = Connection(url)

        err = await cls.__try_connect(conn, max_attempts, attempt_delay.total_seconds())
        if err is not None:
            details = "can't connect to rabbitmq"
            raise ConnectionError(details, url) from err

        try:
            yield cls(conn)

        finally:
            await conn.close()

    def __init__(self, connection: Connection) -> None:
        self.__connection = connection

    @asynccontextmanager
    async def provide_publisher(self, options: PublisherOptions | None = None) -> t.AsyncIterator[Publisher]:
        async with self.__provide_channel(options) as channel:
            yield AiormqPublisher(channel, options, self.__gen_message_id, self.__get_now)

    @asynccontextmanager
    async def bind_consumer(self, consumer: Consumer, options: BindingOptions) -> t.AsyncIterator[BoundConsumer]:
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

    @classmethod
    async def __try_connect(cls, conn: Connection, max_attempts: int, attempt_delay: float) -> Exception | None:
        last_error: Exception | None = None

        for i in range(max_attempts):
            try:
                await conn.connect()
            # NOTE: Need to retry connection with several attempts.
            except (ConnectionError, ProtocolSyntaxError) as err:  # noqa: PERF203
                last_error = err
                if i + 1 < max_attempts:
                    await asyncio.sleep(attempt_delay)
            else:
                return None

        return last_error

    @asynccontextmanager
    async def __provide_channel(self, options: ChannelOptions | None) -> t.AsyncIterator[Channel]:
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

        await channel.exchange_declare(
            exchange=name,
            exchange_type=type_,
            durable=durable,
            auto_delete=auto_delete,
        )

        return name, ExchangeOptions(
            name=name,
            type=type_,
            durable=durable,
            auto_delete=auto_delete,
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

        declare_ok = await channel.queue_declare(
            queue=name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
        )
        assert isinstance(declare_ok.queue, str)

        return declare_ok.queue, QueueOptions(
            name=declare_ok.queue,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
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
        return datetime.now(tz=timezone.utc)
