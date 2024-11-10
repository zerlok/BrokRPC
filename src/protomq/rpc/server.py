from __future__ import annotations

import asyncio
import enum
import typing as t
import warnings
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from signal import Signals

from protomq.abc import BinaryConsumer, BinaryPublisher, BoundConsumer
from protomq.broker import Broker
from protomq.options import BindingOptions, ExchangeOptions, QueueOptions, merge_options
from protomq.rpc.abc import HandlerSerializer, UnaryUnaryFunc
from protomq.rpc.handler import AsyncFuncHandler
from protomq.rpc.model import Request, ServerError
from protomq.stringify import to_str_obj


class State(enum.Enum):
    UNKNOWN = enum.auto()
    IDLE = enum.auto()
    STARTUP = enum.auto()
    RUNNING = enum.auto()
    CLEANUP = enum.auto()


class ServerNotInConfigurableStateError(ServerError):
    pass


class ServerStartupError(ServerError, BaseExceptionGroup):
    pass


@dataclass(frozen=True, kw_only=True)
class ServerOptions:
    startup_timeout: timedelta | None = None
    cleanup_timeout: timedelta | None = None


class Server:
    def __init__(
        self,
        broker: Broker,
        options: ServerOptions | None = None,
    ) -> None:
        self.__broker = broker
        self.__options = options

        self.__lock = asyncio.Lock()
        self.__cm_stack = AsyncExitStack()
        self.__state = State.IDLE
        self.__handlers: list[t.AsyncContextManager[BoundConsumer]] = []
        self.__bound_consumers: list[BoundConsumer] = []

    def __str__(self) -> str:
        return to_str_obj(self, state=self.__state.name, broker=self.__broker)

    def __del__(self) -> None:
        if self.__state is not State.IDLE:
            warnings.warn("server was not stopped properly", RuntimeWarning, stacklevel=1)

    @property
    def state(self) -> State:
        return self.__state

    def register_unary_unary_handler[U, V](
        self,
        func: UnaryUnaryFunc[Request[U], V],
        routing_key: str,
        serializer: HandlerSerializer[U, V],
        exchange: ExchangeOptions | None = None,
        queue: QueueOptions | None = None,
    ) -> None:
        if self.__state is not State.IDLE:
            raise ServerNotInConfigurableStateError(self)

        cm = self.__bind_consumer(
            factory=partial(AsyncFuncHandler, func, serializer),
            binding=self.__get_binding_options(exchange, routing_key, queue),
        )
        self.__handlers.append(cm)

    async def start(self) -> None:
        async with self.__lock:
            if self.__state is State.RUNNING:
                return

            self.__cm_stack.callback(self.__bound_consumers.clear)

            self.__state = State.STARTUP
            try:
                bound_consumers = await asyncio.gather(
                    *(self.__cm_stack.enter_async_context(handler_cm) for handler_cm in self.__handlers)
                )

            except Exception:
                self.__state = State.UNKNOWN
                raise

            else:
                self.__bound_consumers.extend(bound_consumers)
                self.__state = State.RUNNING

    async def stop(self) -> None:
        async with self.__lock:
            if self.__state not in {State.UNKNOWN, State.RUNNING}:
                return

            self.__state = State.CLEANUP
            try:
                await self.__cm_stack.aclose()

            except Exception:
                self.__state = State.UNKNOWN
                raise

            else:
                self.__state = State.IDLE

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
        factory: t.Callable[[BinaryPublisher], BinaryConsumer],
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
            queue=merge_options(
                QueueOptions(
                    name=f"requests.{routing_key}",
                ),
                queue,
            ),
        )
