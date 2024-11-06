from __future__ import annotations

import asyncio
import typing as t
from concurrent.futures import Executor
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial

from yarl import URL

from protomq.abc import (
    BoundConsumer,
    ConsumerMiddleware,
    Driver,
    Publisher,
    PublisherMiddleware,
    RawConsumer,
    RawPublisher,
    Serializer,
)
from protomq.builder import ConsumerBuilder, PublisherBuilder, ident
from protomq.message import ConsumerResult, PublisherResult, RawMessage
from protomq.options import BindingOptions, ConnectOptions, ExchangeOptions, PublisherOptions, QueueOptions

type DSNOrConnectOptions = str | URL | ConnectOptions


class Connection(t.AsyncContextManager["Connection"]):
    def __init__(
        self,
        dsn: DSNOrConnectOptions,
        default_exchange: ExchangeOptions | None = None,
        default_queue: QueueOptions | None = None,
        default_publisher_middlewares: t.Sequence[PublisherMiddleware[RawPublisher, RawMessage, PublisherResult]]
        | None = None,
        default_consumer_middlewares: t.Sequence[ConsumerMiddleware[RawConsumer, RawMessage, ConsumerResult]]
        | None = None,
    ) -> None:
        self.__connect: t.Callable[[], t.AsyncContextManager[Driver]] = partial(connect, dsn)
        self.__default_exchange = default_exchange
        self.__default_queue = default_queue
        self.__default_publisher_middlewares: t.Sequence[
            PublisherMiddleware[RawPublisher, RawMessage, PublisherResult]
        ] = default_publisher_middlewares or ()
        self.__default_consumer_middlewares: t.Sequence[ConsumerMiddleware[RawConsumer, RawMessage, ConsumerResult]] = (
            default_consumer_middlewares or ()
        )

        self.__stack = AsyncExitStack()
        self.__lock = asyncio.Lock()
        self.__opened = asyncio.Event()
        self.__driver: Driver | None = None

    async def __aenter__(self) -> Connection:
        await self.open()
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool | None:
        await self.close()
        return None

    async def open(self) -> None:
        if self.__driver is not None:
            return

        async with self.__lock:
            if self.__driver is not None:
                return

            assert not self.is_open
            self.__driver = await self.__stack.enter_async_context(self.__connect())
            self.__opened.set()
            assert self.is_open

    async def close(self) -> None:
        if self.__driver is None:
            return

        async with self.__lock:
            if self.__driver is None:
                return

            assert self.is_open
            try:
                await self.__stack.aclose()

            finally:
                self.__driver = None
                self.__opened.clear()
                assert not self.is_open

    @property
    def is_open(self) -> bool:
        return self.__driver is not None and self.__opened.is_set()

    @property
    def driver(self) -> Driver:
        if self.__driver is None:
            assert not self.is_open
            details = "connection is not open"
            raise RuntimeError(details, self)

        assert self.is_open
        return self.__driver

    @t.overload
    def publisher[T](
        self,
        options: PublisherOptions | None = None,
    ) -> t.AsyncContextManager[RawPublisher]: ...

    @t.overload
    def publisher[T](
        self,
        options: PublisherOptions | None = None,
        *,
        serializer: Serializer[T, RawMessage],
    ) -> t.AsyncContextManager[Publisher[T, PublisherResult]]: ...

    def publisher[T](
        self,
        options: PublisherOptions | None = None,
        *,
        serializer: Serializer[T, RawMessage] | None = None,
    ) -> t.AsyncContextManager[RawPublisher] | t.AsyncContextManager[Publisher[T, PublisherResult]]:
        return self.publisher_builder().add_serializer(serializer).build(options)

    @t.overload
    def consumer[T](
        self,
        consumer: RawConsumer,
        options: BindingOptions,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def consumer[T](
        self,
        consumer: t.Callable[[RawMessage], t.Awaitable[ConsumerResult]],
        options: BindingOptions,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def consumer[T](
        self,
        consumer: t.Callable[[RawMessage], ConsumerResult],
        options: BindingOptions,
        *,
        executor: Executor | None = None,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def consumer[T](
        self,
        consumer: t.Callable[[T], t.Awaitable[ConsumerResult]],
        options: BindingOptions,
        *,
        serializer: Serializer[T, RawMessage],
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def consumer[T](
        self,
        consumer: t.Callable[[T], ConsumerResult],
        options: BindingOptions,
        *,
        serializer: Serializer[T, RawMessage],
        executor: Executor | None = None,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    def consumer[T](
        self,
        consumer: RawConsumer
        | t.Callable[[RawMessage], t.Awaitable[ConsumerResult]]
        | t.Callable[[RawMessage], ConsumerResult]
        | t.Callable[[T], t.Awaitable[ConsumerResult]]
        | t.Callable[[T], ConsumerResult],
        options: BindingOptions,
        serializer: Serializer[T, RawMessage] | None = None,
        executor: Executor | None = None,
    ) -> t.AsyncContextManager[BoundConsumer]:
        return (
            self.consumer_builder()
            .add_serializer(serializer)
            # TODO: remove cast to any
            .build(t.cast(t.Any, consumer), options, executor=executor)
        )

    def publisher_builder(self) -> PublisherBuilder[RawMessage, PublisherResult]:
        return (
            PublisherBuilder(self.__provide_publisher, ident)
            .set_exchange(self.__default_exchange)
            .add_middlewares(*self.__default_publisher_middlewares)
        )

    def consumer_builder(self) -> ConsumerBuilder[RawMessage, ConsumerResult]:
        return (
            ConsumerBuilder(self.__bind_consumer, ident)
            .set_exchange(self.__default_exchange)
            .set_queue(self.__default_queue)
            .add_middlewares(*self.__default_consumer_middlewares)
        )

    @asynccontextmanager
    async def __provide_publisher(self, options: PublisherOptions | None) -> t.AsyncIterator[RawPublisher]:
        await self.__opened.wait()
        assert self.is_open
        async with self.driver.provide_publisher(options) as publisher:
            yield publisher

    @asynccontextmanager
    async def __bind_consumer(self, consumer: RawConsumer, options: BindingOptions) -> t.AsyncIterator[BoundConsumer]:
        await self.__opened.wait()
        assert self.is_open
        async with self.driver.bind_consumer(consumer, options) as bound_consumer:
            yield bound_consumer


def connect(options: DSNOrConnectOptions) -> t.AsyncContextManager[Driver]:
    clean_options = options if isinstance(options, ConnectOptions) else parse_dsn(options)

    if clean_options.driver == "aiormq":
        from protomq.driver.aiormq import AiormqDriver

        return AiormqDriver.connect(clean_options)

    else:
        details = "unsupported driver"
        raise ValueError(details, clean_options)


def parse_dsn(dsn: str | URL) -> ConnectOptions:
    clean_dsn = URL(dsn) if isinstance(dsn, str) else dsn

    parts = clean_dsn.scheme.split("+", maxsplit=1)
    if len(parts) == 2:
        scheme, driver = parts

    else:
        scheme, driver = parts[0], "aiormq"

    return ConnectOptions(driver=driver, url=clean_dsn.with_scheme(scheme))
