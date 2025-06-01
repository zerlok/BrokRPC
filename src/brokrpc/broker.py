from __future__ import annotations

import asyncio
import typing as t
import warnings

if t.TYPE_CHECKING:
    from concurrent.futures import Executor

    from brokrpc.message import BinaryMessage

from contextlib import AsyncExitStack
from functools import partial

from yarl import URL

from brokrpc.abc import (
    BinaryConsumer,
    BinaryPublisher,
    BoundConsumer,
    BrokerDriver,
    Consumer,
    ConsumerMiddleware,
    Publisher,
    PublisherMiddleware,
    Serializer,
)
from brokrpc.builder import ConsumerBuilder, PublisherBuilder, ident
from brokrpc.model import BrokerError, ConsumerResult, PublisherResult
from brokrpc.options import BindingOptions, BrokerOptions, ExchangeOptions, PublisherOptions, QueueOptions
from brokrpc.stringify import to_str_obj

type BrokerConnectOptions = str | URL | t.AsyncContextManager[BrokerDriver] | BrokerOptions


class BrokerIsNotConnectedError(BrokerError):
    pass


class Broker(t.AsyncContextManager["Broker"]):
    def __init__(
        self,
        options: BrokerConnectOptions,
        *,
        default_exchange: ExchangeOptions | None = None,
        default_queue: QueueOptions | None = None,
        default_publisher_middlewares: t.Sequence[PublisherMiddleware[BinaryPublisher, BinaryMessage, PublisherResult]]
        | None = None,
        default_consumer_middlewares: t.Sequence[ConsumerMiddleware[BinaryConsumer, BinaryMessage, ConsumerResult]]
        | None = None,
    ) -> None:
        self.__options = options
        self.__connect: t.Callable[[], t.AsyncContextManager[BrokerDriver]] = partial(connect, options)
        self.__default_exchange = default_exchange
        self.__default_queue = default_queue
        self.__default_publisher_middlewares: t.Sequence[
            PublisherMiddleware[BinaryPublisher, BinaryMessage, PublisherResult]
        ] = default_publisher_middlewares or ()
        self.__default_consumer_middlewares: t.Sequence[
            ConsumerMiddleware[BinaryConsumer, BinaryMessage, ConsumerResult]
        ] = default_consumer_middlewares or ()

        self.__stack = AsyncExitStack()
        self.__lock = asyncio.Lock()
        self.__opened = asyncio.Event()
        self.__driver: BrokerDriver | None = None

    def __del__(self) -> None:
        if self.is_connected:
            warnings.warn("broker was not disconnected properly", RuntimeWarning, stacklevel=2)

    def __str__(self) -> str:
        return to_str_obj(self, is_connected=self.is_connected, driver=self.__driver)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"options={self.__options!r}, "
            f"default_exchange={self.__default_exchange!r}, "
            f"default_queue={self.__default_queue!r}, "
            f"default_publisher_middlewares={self.__default_publisher_middlewares!r}, "
            f"default_consumer_middlewares={self.__default_consumer_middlewares!r}"
            ")"
        )

    async def __aenter__(self) -> t.Self:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool | None:
        await self.disconnect()
        return None

    async def connect(self) -> None:
        if self.__driver is not None:
            return

        async with self.__lock:
            # NOTE: double check, because other coroutine may have entered the lock and set up the driver.
            if self.__driver is not None:
                return  # type: ignore[unreachable]

            assert not self.is_connected
            self.__driver = await self.__stack.enter_async_context(self.__connect())
            self.__opened.set()
            assert self.is_connected

    async def disconnect(self) -> None:
        if self.__driver is None:
            return

        async with self.__lock:
            # NOTE: double check, because other coroutine may have entered the lock and tear down the driver.
            if self.__driver is None:
                return  # type: ignore[unreachable]

            assert self.is_connected
            try:
                await self.__stack.aclose()

            finally:
                self.__driver = None
                self.__opened.clear()
                assert not self.is_connected

    @property
    def is_connected(self) -> bool:
        return self.__driver is not None and self.__opened.is_set()

    @t.overload
    def publisher[T](
        self,
        options: PublisherOptions | None = None,
    ) -> t.AsyncContextManager[BinaryPublisher]: ...

    @t.overload
    def publisher[T](
        self,
        options: PublisherOptions | None = None,
        *,
        serializer: t.Callable[[T], BinaryMessage] | Serializer[T, BinaryMessage],
    ) -> t.AsyncContextManager[Publisher[T, PublisherResult]]: ...

    def publisher[T](
        self,
        options: PublisherOptions | None = None,
        *,
        serializer: t.Callable[[T], BinaryMessage] | Serializer[T, BinaryMessage] | None = None,
    ) -> t.AsyncContextManager[BinaryPublisher] | t.AsyncContextManager[Publisher[T, PublisherResult]]:
        return self.build_publisher().add_serializer(serializer).build(options)

    @t.overload
    def consumer[T](
        self,
        consumer: BinaryConsumer,
        options: BindingOptions,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def consumer[T](
        self,
        consumer: t.Callable[[BinaryMessage], t.Awaitable[ConsumerResult]],
        options: BindingOptions,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def consumer[T](
        self,
        consumer: t.Callable[[BinaryMessage], ConsumerResult],
        options: BindingOptions,
        *,
        executor: Executor | None = None,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def consumer[T](
        self,
        consumer: Consumer[T, ConsumerResult],
        options: BindingOptions,
        *,
        serializer: t.Callable[[BinaryMessage], T] | Serializer[T, BinaryMessage],
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def consumer[T](
        self,
        consumer: t.Callable[[T], t.Awaitable[ConsumerResult]],
        options: BindingOptions,
        *,
        serializer: t.Callable[[BinaryMessage], T] | Serializer[T, BinaryMessage],
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def consumer[T](
        self,
        consumer: t.Callable[[T], ConsumerResult],
        options: BindingOptions,
        *,
        serializer: t.Callable[[BinaryMessage], T] | Serializer[T, BinaryMessage],
        executor: Executor | None = None,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    # TODO: find a way to write a type safe implementation that invokes `ConsumerBuilder` methods without duplicating
    #  the code of builder.
    def consumer[T](
        self,
        consumer: BinaryConsumer
        | t.Callable[[BinaryMessage], t.Awaitable[ConsumerResult]]
        | t.Callable[[BinaryMessage], ConsumerResult]
        | Consumer[T, ConsumerResult]
        | t.Callable[[T], t.Awaitable[ConsumerResult]]
        | t.Callable[[T], ConsumerResult],
        options: BindingOptions,
        serializer: t.Callable[[BinaryMessage], T] | Serializer[T, BinaryMessage] | None = None,
        executor: Executor | None = None,
    ) -> t.AsyncContextManager[BoundConsumer]:
        return (
            self.build_consumer()
            .add_serializer(serializer)
            .build(
                # NOTE: see Broker.consume method todo
                inner=t.cast(t.Any, consumer),  # type: ignore[misc]
                options=options,
                executor=executor,
            )
        )

    def build_publisher(self) -> PublisherBuilder[BinaryMessage, PublisherResult]:
        return (
            PublisherBuilder(self.__provide_publisher, ident)
            .set_exchange(self.__default_exchange)
            .add_middlewares(*self.__default_publisher_middlewares)
        )

    def build_consumer(self) -> ConsumerBuilder[BinaryMessage, ConsumerResult]:
        return (
            ConsumerBuilder(self.__bind_consumer, ident)
            .set_exchange(self.__default_exchange)
            .set_queue(self.__default_queue)
            .add_middlewares(*self.__default_consumer_middlewares)
        )

    def __get_driver(self) -> BrokerDriver:
        if self.__driver is None:
            assert not self.is_connected
            raise BrokerIsNotConnectedError(self)

        assert self.is_connected
        return self.__driver

    def __provide_publisher(self, options: PublisherOptions | None) -> t.AsyncContextManager[BinaryPublisher]:
        return self.__get_driver().provide_publisher(options)

    def __bind_consumer(
        self,
        consumer: BinaryConsumer,
        options: BindingOptions,
    ) -> t.AsyncContextManager[BoundConsumer]:
        return self.__get_driver().bind_consumer(consumer, options)


def connect(options: BrokerConnectOptions) -> t.AsyncContextManager[BrokerDriver]:
    if isinstance(options, t.AsyncContextManager):
        return options

    clean_options = options if isinstance(options, BrokerOptions) else parse_options(options)

    if clean_options.driver == "aiormq":
        from brokrpc.driver.aiormq import AiormqBrokerDriver

        return AiormqBrokerDriver.connect(clean_options)

    elif clean_options.driver == "memory":
        from brokrpc.driver.memory import InMemoryBrokerDriver

        return InMemoryBrokerDriver.setup(clean_options)

    else:
        details = "unsupported driver"
        raise ValueError(details, clean_options)


def parse_options(url: str | URL) -> BrokerOptions:
    clean_url = URL(url) if isinstance(url, str) else url

    parts = clean_url.scheme.split("+", maxsplit=1)
    match parts:
        case (scheme, driver):
            return BrokerOptions(driver=driver, url=clean_url.with_scheme(scheme))

        case _:
            return BrokerOptions(driver="aiormq", url=clean_url)
