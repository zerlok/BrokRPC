from __future__ import annotations

import asyncio
import typing as t
import warnings
from collections import OrderedDict
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial
from signal import Signals

from protomq.abc import BoundConsumer, RawConsumer, RawPublisher
from protomq.broker import Broker
from protomq.options import BindingOptions, ExchangeOptions, QueueOptions
from protomq.rpc.abc import HandlerFunc, HandlerSerializer, UnaryUnaryFunc
from protomq.rpc.handler import AsyncFuncHandler

U_contra = t.TypeVar("U_contra", contravariant=True)
V_co = t.TypeVar("V_co", covariant=True)


class Server:
    def __init__(self, broker: Broker) -> None:
        self.__broker = broker
        self.__lock = asyncio.Lock()
        self.__cm_stack = AsyncExitStack()
        self.__handlers: t.OrderedDict[object, t.AsyncContextManager[BoundConsumer]] = OrderedDict()
        self.__bound_consumers: list[BoundConsumer] = []

    def __del__(self) -> None:
        if self.is_running:
            warnings.warn("server was not stopped properly", RuntimeWarning, stacklevel=1)

    @property
    def is_running(self) -> bool:
        return len(self.__bound_consumers) > 0

    def register_unary_unary_handler[U_contra, V_co](
        self,
        func: UnaryUnaryFunc[U_contra, V_co],
        routing_key: str,
        serializer: HandlerSerializer[U_contra, V_co],
        exchange: ExchangeOptions | None = None,
        queue: QueueOptions | None = None,
    ) -> None:
        cm = self.__bind_consumer(
            factory=partial(AsyncFuncHandler, func, serializer),
            binding=self.__get_binding_options(exchange, routing_key, queue),
        )
        self.__add_handler(func, cm)

    async def start(self) -> None:
        async with self.__lock:
            if self.is_running:
                return

            self.__cm_stack.callback(self.__bound_consumers.clear)

            self.__bound_consumers.extend(
                await asyncio.gather(
                    *(self.__cm_stack.enter_async_context(handler_cm) for handler_cm in self.__handlers.values())
                )
            )
            assert self.is_running

    async def stop(self) -> None:
        async with self.__lock:
            if not self.is_running:
                return

            await self.__cm_stack.aclose()
            assert not self.is_running

    @asynccontextmanager
    async def run(self) -> t.AsyncIterator[Server]:
        try:
            await self.start()
            yield self

        finally:
            await self.stop()

    async def run_until_done(self, done: asyncio.Event) -> None:
        async with self.run():
            await done.wait()

    async def run_until_terminated(
        self,
        signals: t.Collection[Signals] = (Signals.SIGTERM, Signals.SIGINT),
    ) -> None:
        done = asyncio.Event()

        loop = asyncio.get_running_loop()

        for sig in signals:
            loop.add_signal_handler(sig, done.set)

        try:
            await self.run_until_done(done)

        finally:
            for sig in signals:
                loop.remove_signal_handler(sig)

    @asynccontextmanager
    async def __bind_consumer(
        self,
        factory: t.Callable[[RawPublisher], RawConsumer],
        binding: BindingOptions,
    ) -> t.AsyncIterator[BoundConsumer]:
        async with (
            self.__broker.publisher() as replier,
            self.__broker.consumer(factory(replier), binding) as bound_consumer,
        ):
            yield bound_consumer

    def __get_binding_options(
        self,
        exchange: ExchangeOptions | None,
        routing_key: str,
        queue: QueueOptions | None,
    ) -> BindingOptions:
        return BindingOptions(
            exchange=exchange,
            binding_keys=(routing_key,),
            queue=queue
            if queue is not None
            else QueueOptions(
                name=f"requests.{routing_key}",
            ),
        )

    def __add_handler(self, func: HandlerFunc[t.Any, t.Any], cm: t.AsyncContextManager[BoundConsumer]) -> None:
        if (registered := self.__handlers.get(func)) is not None:
            raise ConsumerAlreadyRegisteredError(func, registered)

        self.__handlers[func] = cm
