from __future__ import annotations

import asyncio
import logging
import typing as t
from contextlib import asynccontextmanager, nullcontext, suppress
from uuid import uuid4

from redis import ConnectionError as RedisConnectionError
from redis import RedisError
from redis.asyncio import Redis

if t.TYPE_CHECKING:
    from datetime import datetime, timedelta
    from types import TracebackType

    from redis.asyncio.client import PubSub

    from brokrpc.options import BindingOptions, BrokerOptions, PublisherOptions

from brokrpc.abc import BinaryConsumer, BinaryPublisher, BoundConsumer, BrokerDriver, Publisher
from brokrpc.errors import ErrorTransformer
from brokrpc.message import BinaryMessage, Message
from brokrpc.model import BrokerConnectionError, BrokerError, PublisherResult
from brokrpc.retry import RetryerError, create_delay_retryer
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


class RedisMessage(Message[bytes]):
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


class RedisPublisher(Publisher[BinaryMessage, PublisherResult]):
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
        await self.__redis.publish(message.routing_key, message.body)
        return None


class RedisSubscriber(t.AsyncContextManager["RedisSubscriber"], BoundConsumer):
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
                    await self.__consumer.consume(RedisMessage(cmd))

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


# TODO: consider streams https://redis.readthedocs.io/en/latest/examples/redis-stream-example.html
class RedisBrokerDriver(BrokerDriver):
    @classmethod
    @asynccontextmanager
    async def connect(cls, options: BrokerOptions) -> t.AsyncIterator[RedisBrokerDriver]:
        def log_warning(err: Exception) -> None:
            logging.warning("can't connect to redis", exc_info=err)

        retryer = create_delay_retryer(
            options=options,
            errors=(ConnectionError, RedisError),
            on_attempt_error=log_warning,
        )

        redis: Redis = Redis.from_url(str(options.url))
        with _ERROR_TRANSFORMER:
            try:
                await retryer.do(redis.initialize)
                yield cls(redis)

            except RetryerError as err:
                details = "can't connect to redis"
                raise BrokerConnectionError(details, options) from err

            finally:
                await redis.aclose()

    def __init__(self, redis: Redis) -> None:
        self.__redis = redis

    def __str__(self) -> str:
        return to_str_obj(self, redis=self.__redis)

    def provide_publisher(self, options: PublisherOptions | None = None) -> t.AsyncContextManager[BinaryPublisher]:
        return nullcontext(RedisPublisher(self.__redis, options, self.__gen_message_id))

    @asynccontextmanager
    async def bind_consumer(self, consumer: BinaryConsumer, options: BindingOptions) -> t.AsyncIterator[BoundConsumer]:
        async with (
            _ERROR_TRANSFORMER,
            self.__redis.pubsub() as pubsub,
            RedisSubscriber(pubsub, consumer, options) as subscriber,
        ):
            yield subscriber

    def __gen_message_id(self) -> str:
        return uuid4().hex
