from __future__ import annotations

import asyncio
import typing as t
import warnings
from collections import OrderedDict

if t.TYPE_CHECKING:
    from concurrent.futures import Executor

from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, replace
from signal import Signals

from brokrpc.rpc.sync_to_async import Sync2AsyncNormalizer

if t.TYPE_CHECKING:
    from yarl import URL

    from brokrpc.abc import (
        BoundConsumer,
        BrokerDriver,
        Consumer,
        Publisher,
        Serializer,
    )
    from brokrpc.rpc.abc import StreamStreamFunc, StreamUnaryFunc, UnaryStreamFunc, UnaryUnaryFunc

from brokrpc.broker import connect
from brokrpc.consumer import ConsumerExceptionDecorator
from brokrpc.model import BrokerError
from brokrpc.options import BindingOptions, ConsumerOptions, QueueOptions
from brokrpc.rpc.consumer import UnaryUnaryConsumer


class ServerConfigurationError(BrokerError):
    pass


class ConsumerAlreadyRegisteredError(ServerConfigurationError):
    pass


class ServerIsAlreadyRunningError(BrokerError):
    pass


@dataclass(frozen=True, kw_only=True)
class BaseConsumerConfig[U, V]:
    serializer: Serializer[U, V]
    bindings: BindingOptions
    consumer: ConsumerOptions


@dataclass(frozen=True, kw_only=True)
class UnaryUnaryConsumerConfig[U, V](BaseConsumerConfig[U, V]):
    func: UnaryUnaryFunc[U, V]


@dataclass(frozen=True, kw_only=True)
class UnaryStreamConsumerConfig[U, V](BaseConsumerConfig[U, V]):
    func: UnaryStreamFunc[U, V]


@dataclass(frozen=True, kw_only=True)
class StreamUnaryConsumerConfig[U, V](BaseConsumerConfig[U, V]):
    func: StreamUnaryFunc[U, V]


@dataclass(frozen=True, kw_only=True)
class StreamStreamConsumerConfig[U, V](BaseConsumerConfig[U, V]):
    func: StreamStreamFunc[U, V]


type ConsumerConfig[U, V] = (
    UnaryUnaryConsumerConfig[U, V]
    # TODO: support other consumers
    # | UnaryStreamConsumerConfig[U, V]
    # | StreamUnaryConsumerConfig[U, V]
    # | StreamStreamConsumerConfig[U, V]
)


class Server:
    @classmethod
    @asynccontextmanager
    async def connect(cls, url: str | URL) -> t.AsyncIterator[Server]:
        async with connect(url) as driver:
            yield cls(driver)

    def __init__(self, driver: BrokerDriver, executor: Executor | None = None) -> None:
        self.__driver = driver
        self.__executor = executor
        self.__cm_stack = AsyncExitStack()
        self.__consumers: t.OrderedDict[object, ConsumerConfig[t.Any, t.Any]] = OrderedDict()
        self.__bound_consumers: list[BoundConsumer] = []
        self.__sync2async = Sync2AsyncNormalizer(executor)

    def __del__(self) -> None:
        if self.is_running:
            warnings.warn("server was not stopped properly", RuntimeWarning, stacklevel=1)

    @property
    def is_running(self) -> bool:
        return len(self.__bound_consumers) > 0

    def register_consumer[U, V](
        self,
        config: ConsumerConfig[U, V],
    ) -> None:
        if (registered := self.__consumers.get(config.func)) is not None:
            raise ConsumerAlreadyRegisteredError(config, registered)

        self.__consumers[config.func] = config

    def register_unary_unary_consumer[U, V](
        self,
        *,
        func: UnaryUnaryFunc[U, V] | t.Callable[[U], V],
        serializer: Serializer[U, V],
        bindings: BindingOptions,
    ) -> None:
        consumer = ConsumerOptions.load(func)

        self.register_consumer(
            UnaryUnaryConsumerConfig(
                func=self.__sync2async.normalize_unary_unary(func),
                serializer=serializer,
                bindings=replace(
                    bindings,
                    queue=self.__collect_queue_options(consumer, bindings.queue),
                ),
                consumer=consumer,
            )
        )

    def register_unary_stream_consumer[U, V](
        self,
        *,
        func: UnaryStreamFunc[U, V] | t.Callable[[U], t.Iterable[V]],
        serializer: Serializer[U, V],
        bindings: BindingOptions,
    ) -> None:
        raise NotImplementedError
        # consumer = ConsumerOptions.load(func)
        #
        # self.register_consumer(
        #     UnaryStreamConsumerConfig(
        #         func=self.__sync2async.normalize_unary_stream(func),
        #         serializer=serializer,
        #         bindings=replace(
        #             bindings,
        #             queue=self.__collect_queue_options(consumer, bindings.queue),
        #         ),
        #         consumer=consumer,
        #     )
        # )

    def register_stream_unary_consumer[U, V](
        self,
        *,
        func: StreamUnaryFunc[U, V],
        serializer: Serializer[U, V],
        bindings: BindingOptions,
    ) -> None:
        raise NotImplementedError

    def register_stream_stream_consumer[U, V](
        self,
        *,
        func: StreamStreamFunc[U, V],
        serializer: Serializer[U, V],
        bindings: BindingOptions,
    ) -> None:
        raise NotImplementedError

    async def start(self) -> None:
        if self.is_running:
            raise ServerIsAlreadyRunningError(self.__bound_consumers)

        self.__cm_stack.callback(self.__bound_consumers.clear)

        responder = await self.__cm_stack.enter_async_context(self.__driver.provide_publisher())
        consumers = [
            (self.__build_consumer(config, responder), config.bindings) for config in self.__consumers.values()
        ]

        self.__bound_consumers.extend(
            await asyncio.gather(
                *(
                    self.__cm_stack.enter_async_context(self.__driver.bind_consumer(consumer, bindings))
                    for consumer, bindings in consumers
                )
            )
        )

    async def stop(self) -> None:
        await self.__cm_stack.aclose()
        assert not self.is_running

    def __build_consumer(self, config: ConsumerConfig[object, object], responder: Publisher) -> Consumer:
        consumer: Consumer

        match config:
            case UnaryUnaryConsumerConfig():
                consumer = UnaryUnaryConsumer(
                    inner=config.func,
                    serializer=config.serializer,
                    responder=responder,
                )

            case _:
                t.assert_never(config)

        return ConsumerExceptionDecorator.from_consume_options(consumer, config.consumer)

    def __collect_queue_options(self, consumer: ConsumerOptions, queue: QueueOptions | None) -> QueueOptions:
        return QueueOptions(
            name=consumer.name if consumer.name is not None else queue.name if queue is not None else None,
            durable=consumer.durable if consumer.durable is not None else queue.durable if queue is not None else None,
            exclusive=consumer.exclusive
            if consumer.exclusive is not None
            else queue.exclusive
            if queue is not None
            else None,
            auto_delete=consumer.auto_delete
            if consumer.auto_delete is not None
            else queue.auto_delete
            if queue is not None
            else None,
            prefetch_count=consumer.prefetch_count
            if consumer.prefetch_count is not None
            else queue.prefetch_count
            if queue is not None
            else None,
        )


async def run_server_until_termination(
    server: Server,
    signals: t.Collection[Signals] = (Signals.SIGTERM, Signals.SIGINT),
) -> None:
    done = asyncio.Event()

    loop = asyncio.get_running_loop()

    for sig in signals:
        loop.add_signal_handler(sig, done.set)

    try:
        await server.start()
        await done.wait()

    finally:
        try:
            await server.stop()

        finally:
            for sig in signals:
                loop.remove_signal_handler(sig)
