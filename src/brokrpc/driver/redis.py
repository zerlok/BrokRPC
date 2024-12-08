from __future__ import annotations

import asyncio
import logging
import typing as t
from contextlib import asynccontextmanager, nullcontext, suppress
from dataclasses import replace
from datetime import datetime, timedelta
from uuid import uuid4

from redis import ConnectionError as RedisConnectionError
from redis import RedisError
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ConstantBackoff

if t.TYPE_CHECKING:
    from types import TracebackType

    from redis.asyncio.client import PubSub

from brokrpc.abc import BinaryConsumer, BinaryPublisher, BoundConsumer, BrokerDriver, Publisher
from brokrpc.errors import ErrorTransformer
from brokrpc.message import BinaryMessage, Message
from brokrpc.model import (
    BrokerConnectionError,
    BrokerError,
    ConsumerAck,
    ConsumerReject,
    ConsumerRetry,
    PublisherResult,
)
from brokrpc.options import BindingOptions, BrokerOptions, ExchangeOptions, PublisherOptions, QueueOptions
from brokrpc.stringify import to_str_obj

_ERROR_TRANSFORMER = ErrorTransformer()


# TODO: check redis error transformation
@_ERROR_TRANSFORMER.register
def _transform_conn_err(err: RedisConnectionError) -> BrokerConnectionError:
    return BrokerConnectionError(str(err))


@_ERROR_TRANSFORMER.register
def _transform_generic_err(err: RedisError) -> BrokerError:
    return BrokerError(str(err))


class RedisPubSubSubscribeCommand(t.TypedDict):
    type: t.Literal["subscribe"]
    pattern: str | None
    channel: bytes
    data: int


class RedisPubSubPatternSubscribeCommand(t.TypedDict):
    type: t.Literal["psubscribe"]
    pattern: str | None
    channel: bytes
    data: int


class RedisPubSubMessageCommand(t.TypedDict):
    type: t.Literal["message"]
    pattern: str | None
    channel: bytes
    data: bytes


class RedisPubSubPatternMessageCommand(t.TypedDict):
    type: t.Literal["pmessage"]
    pattern: str | None
    channel: bytes
    data: bytes


class RedisPubSubUnsubscribeCommand(t.TypedDict):
    type: t.Literal["unsubscribe"]
    pattern: str | None
    channel: bytes
    data: int


class RedisPubSubPatternUnsubscribeCommand(t.TypedDict):
    type: t.Literal["punsubscribe"]
    pattern: str | None
    channel: bytes
    data: int


RedisPubSubCommand = (
    RedisPubSubSubscribeCommand
    | RedisPubSubPatternSubscribeCommand
    | RedisPubSubMessageCommand
    | RedisPubSubPatternMessageCommand
    | RedisPubSubUnsubscribeCommand
    | RedisPubSubPatternUnsubscribeCommand
)
"""
type: One of the following: `subscribe`, `unsubscribe`, `psubscribe`, `punsubscribe`, `message`, `pmessage`

channel: The channel [un]subscribed to or the channel a message was published to

pattern: The pattern that matched a published message's channel. Will be None in all cases except for `pmessage` types.

data: The message data. With [un]subscribe messages, this value will be the number of channels and patterns the
connection is currently subscribed to. With [p]message messages, this value will be the actual published message.
"""


class RedisPubSubMessage(Message[bytes]):
    __slots__ = ("__impl",)

    def __init__(self, impl: RedisPubSubMessageCommand | RedisPubSubPatternMessageCommand) -> None:
        self.__impl = impl

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__impl!r})"

    @property
    def body(self) -> bytes:
        return self.__impl["data"]

    @property
    def routing_key(self) -> str:
        return self.__impl["channel"].decode()

    @property
    def exchange(self) -> str | None:
        return None

    @property
    def content_type(self) -> str | None:
        return None

    @property
    def content_encoding(self) -> str | None:
        return None

    @property
    def headers(self) -> t.Mapping[str, str] | None:
        return None

    @property
    def delivery_mode(self) -> int | None:
        return None

    @property
    def priority(self) -> int | None:
        return None

    @property
    def correlation_id(self) -> str | None:
        return None

    @property
    def reply_to(self) -> str | None:
        return None

    @property
    def timeout(self) -> timedelta | None:
        return None

    @property
    def message_id(self) -> str | None:
        return None

    @property
    def timestamp(self) -> datetime | None:
        return None

    @property
    def message_type(self) -> str | None:
        return None

    @property
    def user_id(self) -> str | None:
        return None

    @property
    def app_id(self) -> str | None:
        return None


class RedisStreamMessage(Message[bytes]):
    __slots__ = (
        "__app_id",
        "__body",
        "__content_encoding",
        "__content_type",
        "__correlation_id",
        "__delivery_mode",
        "__exchange",
        "__headers",
        "__message_id",
        "__message_type",
        "__priority",
        "__reply_to",
        "__routing_key",
        "__timeout",
        "__timestamp",
        "__user_id",
    )

    # NOTE: message constructor has a lot of options to set up a structure (dataclass)
    def __init__(  # noqa: PLR0913
        self,
        *,
        body: bytes,
        routing_key: str,
        exchange: str | None = None,
        content_type: str | None = None,
        content_encoding: str | None = None,
        headers: t.Mapping[str, str] | None = None,
        delivery_mode: int | None = None,
        priority: int | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        timeout: timedelta | None = None,
        message_id: str | None = None,
        timestamp: datetime | None = None,
        message_type: str | None = None,
        user_id: str | None = None,
        app_id: str | None = None,
    ) -> None:
        self.__body = body
        self.__routing_key = routing_key
        self.__exchange = exchange
        self.__content_type = content_type
        self.__content_encoding = content_encoding
        self.__headers = headers
        self.__delivery_mode = delivery_mode
        self.__priority = priority
        self.__correlation_id = correlation_id
        self.__reply_to = reply_to
        self.__timeout = timeout
        self.__message_id = message_id
        self.__timestamp = timestamp
        self.__message_type = message_type
        self.__user_id = user_id
        self.__app_id = app_id

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"body={self.__body!r}, "
            f"routing_key={self.__routing_key!r}, "
            f"exchange={self.__exchange!r}, "
            f"content_type={self.__content_type!r}, "
            f"content_encoding={self.__content_encoding!r}, "
            f"headers={self.__headers!r}, "
            f"delivery_mode={self.__delivery_mode!r}, "
            f"priority={self.__priority!r}, "
            f"correlation_id={self.__correlation_id!r}, "
            f"reply_to={self.__reply_to!r}, "
            f"timeout={self.__timeout!r}, "
            f"message_id={self.__message_id!r}, "
            f"timestamp={self.__timestamp!r}, "
            f"message_type={self.__message_type!r}, "
            f"user_id={self.__user_id!r}, "
            f"app_id={self.__app_id!r}"
            ")"
        )

    @property
    def body(self) -> bytes:
        return self.__body

    @property
    def routing_key(self) -> str:
        return self.__routing_key

    @property
    def exchange(self) -> str | None:
        return self.__exchange

    @property
    def content_type(self) -> str | None:
        return self.__content_type

    @property
    def content_encoding(self) -> str | None:
        return self.__content_encoding

    @property
    def headers(self) -> t.Mapping[str, str] | None:
        return self.__headers

    @property
    def delivery_mode(self) -> int | None:
        return self.__delivery_mode

    @property
    def priority(self) -> int | None:
        return self.__priority

    @property
    def correlation_id(self) -> str | None:
        return self.__correlation_id

    @property
    def reply_to(self) -> str | None:
        return self.__reply_to

    @property
    def timeout(self) -> timedelta | None:
        return self.__timeout

    @property
    def message_id(self) -> str | None:
        return self.__message_id

    @property
    def timestamp(self) -> datetime | None:
        return self.__timestamp

    @property
    def message_type(self) -> str | None:
        return self.__message_type

    @property
    def user_id(self) -> str | None:
        return self.__user_id

    @property
    def app_id(self) -> str | None:
        return self.__app_id


@t.overload
def _dump_key(options: BindingOptions) -> bytes: ...


@t.overload
def _dump_key(options: str | ExchangeOptions | None, routing_key: str) -> bytes: ...


def _dump_key(options: str | ExchangeOptions | BindingOptions | None, routing_key: str | None = None) -> bytes:
    exchange_name = (
        options.exchange.name
        if isinstance(options, BindingOptions) and options.exchange is not None
        else options.name
        if isinstance(options, ExchangeOptions)
        else options
    )
    rk = options.queue.name if isinstance(options, BindingOptions) and options.queue is not None else routing_key
    assert rk is not None

    return rk.encode() if not exchange_name else f"{exchange_name}:{rk}".encode()


def _extract_key(key: bytes) -> tuple[str | None, str]:
    *other, rk = key.decode().split(":", maxsplit=1)
    return other[0] if other else None, rk


class RedisPubSubPublisher(Publisher[BinaryMessage, PublisherResult]):
    def __init__(
        self,
        redis: Redis,
        options: PublisherOptions | None,
        message_id_gen: t.Callable[[], str | None],
    ) -> None:
        self.__redis = redis
        self.__options = options
        self.__message_id_gen = message_id_gen

    @_ERROR_TRANSFORMER.wrap
    async def publish(self, message: BinaryMessage) -> PublisherResult:
        # TODO: find a way to pass RPC metadata with redis message. JSON & protobuf imposes additional dependencies to
        #  server & client. Also, keep in mind RPC abstraction from language implementations (not python only).
        assert message.correlation_id is None
        assert message.reply_to is None
        await self.__redis.publish(_dump_key(message.exchange or self.__options, message.routing_key), message.body)
        return None


class RedisStreamPublisher(Publisher[BinaryMessage, PublisherResult]):
    def __init__(
        self,
        redis: Redis,
        options: PublisherOptions | None,
    ) -> None:
        self.__redis = redis
        self.__options = options

    @_ERROR_TRANSFORMER.wrap
    async def publish(self, message: BinaryMessage) -> PublisherResult:
        await self.__redis.xadd(
            name=_dump_key(message.exchange or self.__options, message.routing_key),
            fields={
                b"body": message.body,
                b"content_type": message.content_type or b"",
                b"content_encoding": message.content_encoding or b"",
                **{f"header.{key}".encode(): value.encode() for key, value in (message.headers or {}).items()},
                b"delivery_mode": message.delivery_mode or b"",
                b"priority": message.priority or b"",
                b"correlation_id": message.correlation_id or b"",
                b"reply_to": message.reply_to or b"",
                # b"timeout": message.timeout or b"",
                b"timestamp": message.timestamp.isoformat() if message.timestamp is not None else b"",
                b"message_type": message.message_type or b"",
                b"user_id": message.user_id or b"",
                b"app_id": message.app_id or b"",
            },
            id=message.message_id if message.message_id is not None else "*",
        )

        return None


class RedisPubSubSubscriber(t.AsyncContextManager["RedisPubSubSubscriber"], BoundConsumer):
    def __init__(
        self,
        pubsub: PubSub,
        consumer: BinaryConsumer,
        options: BindingOptions,
    ) -> None:
        self.__pubsub = pubsub
        self.__consumer = consumer
        self.__options = options

        self.__task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> t.Self:
        if self.__task is not None:
            raise RuntimeError

        await self.__pubsub.subscribe(*self.__options.binding_keys)
        self.__task = asyncio.create_task(self.__run())

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        try:
            # Redis automatically handles message dispatch, so no need for manual consumer cancellation
            await self.__pubsub.unsubscribe()

        finally:
            task, self.__task = self.__task, None

            # TODO: stop consumer task gracefully
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    def is_alive(self) -> bool:
        return bool(self.__pubsub.subscribed) and self.__task is not None and not self.__task.done()

    def get_options(self) -> BindingOptions:
        return self.__options

    @_ERROR_TRANSFORMER.wrap
    async def __run(self) -> None:
        cmd: RedisPubSubCommand

        async for cmd in self.__pubsub.listen():
            assert isinstance(cmd, dict)  # todo: ensure typing is valid

            if cmd["type"] == "subscribe":
                # todo subscribe
                pass

            elif cmd["type"] == "psubscribe":
                # todo psubscribe
                pass

            elif cmd["type"] == "message" or cmd["type"] == "pmessage":
                try:
                    await self.__consumer.consume(RedisPubSubMessage(cmd))

                except Exception as err:
                    logging.exception("fatal consumer error", exc_info=err)

            elif cmd["type"] == "unsubscribe":
                # todo unsubscribe
                pass

            elif cmd["type"] == "punsubscribe":
                # todo punsubscribe
                pass

            else:
                t.assert_never(cmd["type"])


class RedisStreamConsumerGroupSubscriber(t.AsyncContextManager["RedisStreamConsumerGroupSubscriber"], BoundConsumer):
    def __init__(
        self,
        redis: Redis,
        consumer: BinaryConsumer,
        options: BindingOptions,
        consumer_group_id: str,
        consumer_id: str,
    ) -> None:
        self.__redis = redis
        self.__consumer = consumer
        self.__options = replace(
            options,
            queue=replace(options.queue if options.queue is not None else QueueOptions(), name=consumer_group_id),
        )
        self.__consumer_group_id = consumer_group_id
        self.__consumer_id = consumer_id
        # TODO: make stream ID configurable?
        self.__streams: dict[bytes, bytes] = {
            _dump_key(self.__options.exchange, key): b">" for key in self.__options.binding_keys
        }

        self.__task: asyncio.Task[None] | None = None

    @_ERROR_TRANSFORMER.wrap
    async def __aenter__(self) -> t.Self:
        if self.__task is not None:
            raise RuntimeError

        # TODO: get deeper understanding of stream arg passed to XGROUP CREATE & stream args passed to XREADGROUP
        #  https://github.com/redis/redis/issues/9523 - may help
        #  https://github.com/redis/redis/discussions/13680
        await asyncio.gather(
            *(
                self.__redis.xgroup_create(
                    name=stream,
                    groupname=self.__consumer_group_id,
                    mkstream=True,
                )
                for stream in self.__streams
            ),
            # TODO: suppress only `group already exists error`
            return_exceptions=True,
        )

        self.__task = asyncio.create_task(self.__run())

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        # No need to manually unsubscribe, Redis Streams manage group consumption
        task, self.__task = self.__task, None

        # TODO: stop consumer task gracefully
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    def is_alive(self) -> bool:
        return self.__task is not None and not self.__task.done()

    def get_options(self) -> BindingOptions:
        return self.__options

    @_ERROR_TRANSFORMER.wrap
    async def __run(self) -> None:
        while True:
            read_response = await self.__redis.xreadgroup(
                groupname=self.__consumer_group_id,
                consumername=self.__consumer_id,
                # FIXME: remove type ignore here
                streams=self.__streams,  # type: ignore[arg-type]
                count=self.__options.queue.prefetch_count if self.__options.queue is not None else 1,
                # TODO: make it configurable
                block=300_000,  # 5 minutes
            )

            for stream, stream_id, message in self.__extract_messages(read_response):
                try:
                    consumer_result = await self.__consumer.consume(message)

                except Exception as err:
                    logging.exception("fatal consumer error", exc_info=err)

                else:
                    match consumer_result:
                        case ConsumerAck() | True | None:
                            await self.__redis.xack(stream, self.__consumer_group_id, stream_id)
                            self.__streams[stream] = stream_id

                        case ConsumerReject() | False:
                            # TODO: reject message
                            pass

                        case ConsumerRetry():
                            # TODO: retry message consumption with delay
                            pass

    def __extract_messages(
        self,
        response: t.Sequence[tuple[bytes, t.Sequence[tuple[bytes, t.Mapping[bytes, bytes]]]]],
    ) -> t.Iterable[tuple[bytes, bytes, RedisStreamMessage]]:
        for key, messages in response:
            exchange, routing_key = _extract_key(key)

            for message_id, fields in messages:
                yield (
                    key,
                    message_id,
                    RedisStreamMessage(
                        body=fields[b"body"],
                        routing_key=routing_key,
                        exchange=exchange,
                        content_type=fields[b"content_type"].decode() if fields[b"content_type"] else None,
                        content_encoding=fields[b"content_encoding"].decode() if fields[b"content_encoding"] else None,
                        headers={
                            key[len(b"header.") :].decode(): value.decode()
                            for key, value in fields.items()
                            if key.startswith(b"header.")
                        },
                        delivery_mode=int.from_bytes(fields[b"delivery_mode"]) if fields[b"delivery_mode"] else None,
                        priority=int.from_bytes(fields[b"priority"]) if fields[b"delivery_mode"] else None,
                        correlation_id=fields[b"correlation_id"].decode() if fields[b"correlation_id"] else None,
                        reply_to=fields[b"reply_to"].decode() if fields[b"reply_to"] else None,
                        # TODO: add timeout,
                        message_id=message_id.decode(),
                        timestamp=datetime.fromisoformat(fields[b"timestamp"].decode())
                        if fields[b"timestamp"]
                        else None,
                        message_type=fields[b"message_type"].decode() if fields[b"message_type"] else None,
                        user_id=fields[b"user_id"].decode() if fields[b"user_id"] else None,
                        app_id=fields[b"app_id"].decode() if fields[b"app_id"] else None,
                    ),
                )


# TODO: consider streams
#  https://redis.readthedocs.io/en/latest/examples/redis-stream-example.html
#  https://redis.io/docs/latest/develop/data-types/streams/
class RedisBrokerDriver(BrokerDriver):
    @classmethod
    @asynccontextmanager
    async def connect(cls, options: BrokerOptions) -> t.AsyncIterator[RedisBrokerDriver]:
        redis: Redis = Redis.from_url(
            url=str(options.url),
            retry=Retry(
                backoff=ConstantBackoff(backoff=3.0),
                retries=10,
                # TODO: use our retry with logging, adapt it to redis error
                supported_errors=(RedisConnectionError, ConnectionError),  # type: ignore[arg-type]
            ),
        )

        with _ERROR_TRANSFORMER:
            try:
                await redis.initialize()
                yield cls(redis)

            finally:
                await redis.aclose()

    def __init__(self, redis: Redis) -> None:
        self.__redis = redis

    def __str__(self) -> str:
        return to_str_obj(self, redis=self.__redis)

    def provide_publisher(self, options: PublisherOptions | None = None) -> t.AsyncContextManager[BinaryPublisher]:
        return nullcontext(RedisStreamPublisher(self.__redis, options))

    @asynccontextmanager
    async def bind_consumer(self, consumer: BinaryConsumer, options: BindingOptions) -> t.AsyncIterator[BoundConsumer]:
        # TODO: use PubSub if queue is not set (no consumer groups)
        assert options.queue is not None
        assert options.queue.name

        async with (
            _ERROR_TRANSFORMER,
            # self.__redis.pubsub() as pubsub,
            RedisStreamConsumerGroupSubscriber(
                self.__redis,
                consumer,
                options,
                consumer_group_id=options.queue.name,
                # TODO: make it configurable
                consumer_id="my-consumer",
            ) as subscriber,
        ):
            yield subscriber

    def __gen_message_id(self) -> str:
        return uuid4().hex
