from __future__ import annotations

import asyncio
import enum
import inspect
import typing as t
import warnings
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from functools import partial
from signal import Signals

if t.TYPE_CHECKING:
    from concurrent.futures import Executor
    from datetime import timedelta

    from brokrpc.broker import Broker
    from brokrpc.message import BinaryMessage

from brokrpc.abc import BinaryConsumer, BinaryPublisher, BoundConsumer, Consumer, Serializer
from brokrpc.model import ConsumerResult
from brokrpc.options import BindingOptions, ExchangeOptions, QueueOptions, merge_options
from brokrpc.rpc.abc import HandlerSerializer, UnaryUnaryHandler
from brokrpc.rpc.handler import AsyncFuncHandler, SyncFuncHandler
from brokrpc.rpc.model import Request, ServerError
from brokrpc.stringify import to_str_obj


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
    executor: Executor | None = None


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
        self.__consumer_cms: list[t.Callable[[], t.AsyncContextManager[BoundConsumer]]] = []
        self.__bound_consumers: list[BoundConsumer] = []

    def __str__(self) -> str:
        return to_str_obj(self, state=self.__state.name, broker=self.__broker)

    def __del__(self) -> None:
        if self.__state is not State.IDLE:
            warnings.warn("server was not stopped properly", RuntimeWarning, stacklevel=1)

    @property
    def state(self) -> State:
        return self.__state

    def register_consumer[U](
        self,
        func: Consumer[U, ConsumerResult]
        | t.Callable[[U], t.Awaitable[ConsumerResult]]
        | t.Callable[[U], ConsumerResult],
        routing_key: str,
        serializer: Serializer[U, BinaryMessage],
        exchange: ExchangeOptions | None = None,
        queue: QueueOptions | None = None,
    ) -> None:
        if self.__state is not State.IDLE:
            raise ServerNotInConfigurableStateError(self)

        binding = self.__get_binding_options(exchange, routing_key, queue)

        cm: t.Callable[[], t.AsyncContextManager[BoundConsumer]]
        match func:
            case consumer if isinstance(consumer, Consumer):
                cm = partial(self.__broker.consumer, consumer, binding, serializer=serializer)

            case async_func if inspect.iscoroutinefunction(async_func):
                cm = partial(self.__broker.consumer, async_func, binding, serializer=serializer)

            case sync_func if callable(sync_func):
                cm = partial(
                    self.__broker.consumer,
                    # TODO: avoid cast
                    t.cast(t.Callable[[U], ConsumerResult], sync_func),
                    binding,
                    serializer=serializer,
                    executor=self.__options.executor if self.__options is not None else None,
                )

            case _:
                t.assert_never(func)

        self.__consumer_cms.append(cm)

    def register_unary_unary_handler[U, V](
        self,
        *,
        func: UnaryUnaryHandler[Request[U], V] | t.Callable[[Request[U]], t.Awaitable[V]] | t.Callable[[Request[U]], V],
        routing_key: str,
        serializer: HandlerSerializer[U, V],
        exchange: ExchangeOptions | None = None,
        queue: QueueOptions | None = None,
    ) -> None:
        if self.__state is not State.IDLE:
            raise ServerNotInConfigurableStateError(self)

        factory: t.Callable[[BinaryPublisher], BinaryConsumer]
        match func:
            case handler if isinstance(handler, UnaryUnaryHandler):
                factory = partial(AsyncFuncHandler, handler.handle, serializer)

            case async_func if inspect.iscoroutinefunction(async_func):
                factory = partial(AsyncFuncHandler, async_func, serializer)

            case sync_func if callable(sync_func):
                factory = partial(
                    SyncFuncHandler,
                    # TODO: avoid cast
                    t.cast(t.Callable[[Request[U]], V], sync_func),
                    serializer,
                    executor=self.__options.executor if self.__options is not None else None,
                )

            case _:
                t.assert_never(func)

        self.__consumer_cms.append(
            partial(
                self.__bind_handler,
                factory=factory,
                binding=self.__get_binding_options(exchange, routing_key, queue),
            )
        )

    async def start(self) -> None:
        async with self.__lock:
            if self.__state is State.RUNNING:
                return

            self.__cm_stack.callback(self.__bound_consumers.clear)

            self.__state = State.STARTUP
            try:
                bound_consumers = await asyncio.gather(
                    *(self.__cm_stack.enter_async_context(cm()) for cm in self.__consumer_cms)
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
    async def __bind_handler(
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
