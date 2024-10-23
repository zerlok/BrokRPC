from __future__ import annotations

import asyncio
import typing as t
import warnings
from collections import OrderedDict
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, replace
from signal import Signals

if t.TYPE_CHECKING:
    from yarl import URL

    from protomq.abc import BoundConsumer, Consumer, Driver, Publisher, Serializer, UnaryUnaryFunc

from protomq.consumer import ConsumerExceptionDecorator, UnaryUnaryConsumer
from protomq.driver.connect import connect
from protomq.exception import ServerError
from protomq.options import BindingOptions, ConsumerOptions, QueueOptions


class ServerConfigurationError(ServerError):
    pass


class ConsumerAlreadyRegisteredError(ServerConfigurationError):
    pass


class ServerIsAlreadyRunningError(ServerError):
    pass


@dataclass(frozen=True, kw_only=True)
class UnaryUnaryConsumerConfig[U, V]:
    func: UnaryUnaryFunc[U, V]
    serializer: Serializer[U, V]
    bindings: BindingOptions
    consumer: ConsumerOptions


type ConsumerConfig = UnaryUnaryConsumerConfig[t.Any, t.Any]


class Server:
    @classmethod
    @asynccontextmanager
    async def connect(cls, url: str | URL) -> t.AsyncIterator[Server]:
        async with connect(url) as driver:
            yield cls(driver)

    def __init__(self, driver: Driver) -> None:
        self.__driver = driver
        self.__cm_stack = AsyncExitStack()
        self.__consumers: t.OrderedDict[object, ConsumerConfig] = OrderedDict()
        self.__bound_consumers: list[BoundConsumer] = []

    def __del__(self) -> None:
        if self.is_running:
            warnings.warn("server was not stopped properly", RuntimeWarning, stacklevel=1)

    @property
    def is_running(self) -> bool:
        return len(self.__bound_consumers) > 0

    def register_unary_unary_consumer[U, V](
        self,
        *,
        func: UnaryUnaryFunc[U, V],
        serializer: Serializer[U, V],
        bindings: BindingOptions,
    ) -> None:
        if (registered := self.__consumers.get(func)) is not None:
            raise ConsumerAlreadyRegisteredError(func, registered)

        consumer = ConsumerOptions.load(func)

        self.__consumers[func] = UnaryUnaryConsumerConfig(
            func=func,
            serializer=serializer,
            bindings=replace(
                bindings,
                queue=self.__collect_queue_options(consumer, bindings.queue),
            ),
            consumer=consumer,
        )

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

    def __build_consumer(self, config: ConsumerConfig, responder: Publisher) -> Consumer:
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
