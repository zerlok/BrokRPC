from __future__ import annotations

import inspect
import typing as t
from concurrent.futures import Executor
from contextlib import asynccontextmanager
from dataclasses import replace

from protomq.abc import (
    BoundConsumer,
    Consumer,
    ConsumerMiddleware,
    Publisher,
    PublisherMiddleware,
    RawConsumer,
    RawPublisher,
    Serializer,
)
from protomq.consumer import AsyncFuncConsumer, DecodingConsumer, SyncFuncConsumer
from protomq.middleware import ConsumerMiddlewareWrapper, PublisherMiddlewareWrapper
from protomq.options import BindingOptions, ExchangeOptions, PublisherOptions, QueueOptions
from protomq.publisher import EncodingPublisher


def ident[T](obj: T) -> T:
    return obj


class PublisherBuilder[U, V]:
    def __init__(
        self,
        provider: t.Callable[[PublisherOptions | None], t.AsyncContextManager[RawPublisher]],
        wrapper: t.Callable[[RawPublisher], Publisher[U, V]],
        exchange: ExchangeOptions | None = None,
    ) -> None:
        self.__provider = provider
        self.__wrapper = wrapper
        self.__exchange = exchange

    def set_exchange(self, exchange: ExchangeOptions | None) -> PublisherBuilder[U, V]:
        return PublisherBuilder(self.__provider, self.__wrapper, exchange)

    @t.overload
    def add_serializer(self, serializer: None) -> PublisherBuilder[U, V]: ...

    @t.overload
    def add_serializer[U2](self, serializer: t.Callable[[U2], U]) -> PublisherBuilder[U2, V]: ...

    @t.overload
    def add_serializer[U2](self, serializer: Serializer[U2, U]) -> PublisherBuilder[U2, V]: ...

    def add_serializer[U2](self, serializer: t.Callable[[U2], U] | Serializer[U2, U] | None) -> PublisherBuilder[U2, V]:
        if serializer is None:
            return t.cast(PublisherBuilder[U2, V], self)

        inner_wrapper = self.__wrapper
        dump_message = serializer.dump_message if isinstance(serializer, Serializer) else serializer

        def wrapper(inner: RawPublisher) -> Publisher[U2, V]:
            return EncodingPublisher(inner_wrapper(inner), dump_message)

        return PublisherBuilder(self.__provider, wrapper, self.__exchange)

    def add_middleware[U2, V2](
        self,
        middleware: PublisherMiddleware[Publisher[U, V], U2, V2],
    ) -> PublisherBuilder[U2, V2]:
        inner_wrapper = self.__wrapper

        def wrapper(inner: RawPublisher) -> Publisher[U2, V2]:
            return PublisherMiddlewareWrapper(inner_wrapper(inner), middleware)

        return PublisherBuilder(self.__provider, wrapper, self.__exchange)

    def add_middlewares(
        self,
        *middlewares: PublisherMiddleware[Publisher[U, V], U, V],
    ) -> PublisherBuilder[U, V]:
        result = self

        for middleware in middlewares:
            result = result.add_middleware(middleware)

        return result

    @asynccontextmanager
    async def build(self, options: PublisherOptions | None = None) -> t.AsyncIterator[Publisher[U, V]]:
        async with self.__provider(self.__clear_options(options)) as publisher:
            yield self.__wrapper(publisher)

    def __clear_options(self, options: PublisherOptions | None) -> PublisherOptions | None:
        if self.__exchange is None:
            return options

        return PublisherOptions(
            prefetch_count=options.prefetch_count if options is not None else self.__exchange.prefetch_count,
            name=options.name if options is not None else self.__exchange.name,
            type=options.type if options is not None else self.__exchange.type,
            durable=options.durable if options is not None else self.__exchange.durable,
            auto_delete=options.auto_delete if options is not None else self.__exchange.auto_delete,
            arguments=options.arguments if options is not None else self.__exchange.arguments,
            mandatory=options.mandatory if options is not None else None,
        )


class ConsumerBuilder[U, V]:
    def __init__(
        self,
        binder: t.Callable[[RawConsumer, BindingOptions], t.AsyncContextManager[BoundConsumer]],
        wrapper: t.Callable[[Consumer[U, V]], RawConsumer],
        exchange: ExchangeOptions | None = None,
        queue: QueueOptions | None = None,
    ) -> None:
        self.__binder = binder
        self.__wrapper = wrapper
        self.__exchange = exchange
        self.__queue = queue

    def set_exchange(self, exchange: ExchangeOptions | None) -> ConsumerBuilder[U, V]:
        return ConsumerBuilder(self.__binder, self.__wrapper, exchange, self.__queue)

    def set_queue(self, queue: QueueOptions | None) -> ConsumerBuilder[U, V]:
        return ConsumerBuilder(self.__binder, self.__wrapper, self.__exchange, queue)

    @t.overload
    def add_serializer(self, serializer: None) -> ConsumerBuilder[U, V]: ...

    @t.overload
    def add_serializer[U2](self, serializer: t.Callable[[U], U2]) -> ConsumerBuilder[U2, V]: ...

    @t.overload
    def add_serializer[U2](self, serializer: Serializer[U2, U]) -> ConsumerBuilder[U2, V]: ...

    def add_serializer[U2](self, serializer: t.Callable[[U], U2] | Serializer[U2, U] | None) -> ConsumerBuilder[U2, V]:
        if serializer is None:
            return t.cast(ConsumerBuilder[U2, V], self)

        inner_wrapper = self.__wrapper
        load_message = serializer.load_message if isinstance(serializer, Serializer) else serializer

        def wrapper(inner: Consumer[U2, V]) -> RawConsumer:
            return inner_wrapper(DecodingConsumer(inner, load_message))

        return ConsumerBuilder(self.__binder, wrapper, self.__exchange, self.__queue)

    def add_middleware(
        self,
        middleware: ConsumerMiddleware[Consumer[U, V], U, V],
    ) -> ConsumerBuilder[U, V]:
        inner_wrapper = self.__wrapper

        def wrapper(inner: Consumer[U, V]) -> RawConsumer:
            return inner_wrapper(ConsumerMiddlewareWrapper(inner, middleware))

        return ConsumerBuilder(self.__binder, wrapper, self.__exchange, self.__queue)

    def add_middlewares(
        self,
        *middlewares: ConsumerMiddleware[Consumer[U, V], U, V],
    ) -> ConsumerBuilder[U, V]:
        result = self

        for middleware in middlewares:
            result = result.add_middleware(middleware)

        return result

    @t.overload
    def build(
        self,
        inner: Consumer[U, V],
        options: BindingOptions,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def build(
        self,
        inner: t.Callable[[U], t.Awaitable[V]],
        options: BindingOptions,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    @t.overload
    def build(
        self,
        inner: t.Callable[[U], V],
        options: BindingOptions,
        *,
        executor: Executor | None = None,
    ) -> t.AsyncContextManager[BoundConsumer]: ...

    def build(
        self,
        inner: Consumer[U, V] | t.Callable[[U], t.Awaitable[V]] | t.Callable[[U], V],
        options: BindingOptions,
        executor: Executor | None = None,
    ) -> t.AsyncContextManager[BoundConsumer]:
        consumer: Consumer[U, V]

        match inner:
            case Consumer():
                consumer = inner

            case async_func if inspect.iscoroutinefunction(async_func):
                consumer = AsyncFuncConsumer(async_func)

            case sync_func if inspect.isfunction(sync_func):
                consumer = SyncFuncConsumer(t.cast(t.Callable[[U], V], sync_func), executor)

            case _:
                # FIXME: make assert_never work
                # t.assert_never(inner)
                raise TypeError(inner)

        return self.__binder(self.__wrapper(consumer), self.__clear_options(options))

    def __clear_options(self, options: BindingOptions) -> BindingOptions:
        if self.__exchange is None and self.__queue is None:
            return options

        return replace(
            options,
            exchange=options.exchange if options.exchange is not None else self.__exchange,
            queue=options.queue if options.queue is not None else self.__queue,
        )
