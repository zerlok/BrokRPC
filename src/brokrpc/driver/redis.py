from __future__ import annotations

import asyncio
import logging
import typing as t
from asyncio import TaskGroup
from contextlib import asynccontextmanager, nullcontext
from datetime import datetime, timedelta
from uuid import uuid4

from redis import RedisError
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from brokrpc.abc import BinaryConsumer, BinaryPublisher, BoundConsumer, BrokerDriver, Publisher
from brokrpc.errors import ErrorTransformer
from brokrpc.message import BinaryMessage, Message
from brokrpc.model import BrokerConnectionError, PublisherResult
from brokrpc.options import BindingOptions, BrokerOptions, PublisherOptions
from brokrpc.retry import RetryerError, create_delay_retryer
from brokrpc.stringify import to_str_obj

_ERROR_TRANSFORMER = ErrorTransformer()

# TODO: transform redis errors to BrokerError
# @_ERROR_TRANSFORMER.register
# def _transform_conn_err(err: Exception) -> BrokerConnectionError:
#     return BrokerConnectionError(str(err))
#
#
# @_ERROR_TRANSFORMER.register
# def _transform_generic_err(err: Exception) -> BrokerError:
#     return BrokerError(str(err))


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
        return "routing_key"

    @property
    def exchange(self) -> str | None:
        return "exchange"

    @property
    def content_type(self) -> str | None:
        return "content_type"

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
        await self.__redis.publish(message.routing_key, message.body)
        return None


class RedisSubscriber:
    def __init__(self, pubsub: PubSub, consumer: BinaryConsumer, done: asyncio.Event) -> None:
        self.__pubsub = pubsub
        self.__consumer = consumer
        self.__done = done

    async def run(self) -> None:
        cmd: RedisPubSubCommand

        # todo: wait for command or done event
        async for cmd in self.__pubsub.listen():
            if self.__done.is_set():
                return

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


class RedisBoundConsumer(BoundConsumer):
    def __init__(
        self,
        redis: Redis,
        pubsub: PubSub,
        subscriber: RedisSubscriber,
        options: BindingOptions,
    ) -> None:
        self.__redis = redis
        self.__pubsub = pubsub
        self.__subscriber = subscriber
        self.__options = options

    def is_alive(self) -> bool:
        return bool(self.__pubsub.subscribed)

    def get_options(self) -> BindingOptions:
        return self.__options


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

        redis = Redis.from_url(str(options.url))
        with _ERROR_TRANSFORMER:
            try:
                await retryer.do(redis.initialize)
                yield cls(redis)

            except RetryerError as err:
                details = "can't connect to redis"
                raise BrokerConnectionError(details, options) from err

            finally:
                await redis.close()

    def __init__(self, redis: Redis) -> None:
        self.__redis = redis

    def __str__(self) -> str:
        return to_str_obj(self, redis=self.__redis)

    def provide_publisher(self, options: PublisherOptions | None = None) -> t.AsyncContextManager[BinaryPublisher]:
        return nullcontext(RedisPublisher(self.__redis, options, self.__gen_message_id))

    @asynccontextmanager
    async def bind_consumer(self, consumer: BinaryConsumer, options: BindingOptions) -> t.AsyncIterator[BoundConsumer]:
        async with _ERROR_TRANSFORMER, self.__redis.pubsub() as pubsub, TaskGroup() as tg:
            done = asyncio.Event()

            subscriber = RedisSubscriber(pubsub, consumer, done)
            try:
                await pubsub.subscribe(*options.binding_keys)
                tg.create_task(subscriber.run())

                yield RedisBoundConsumer(self.__redis, pubsub, subscriber, options)

            finally:
                # Redis automatically handles message dispatch, so no need for manual consumer cancellation
                await pubsub.unsubscribe()
                done.set()

    def __gen_message_id(self) -> str:
        return uuid4().hex
