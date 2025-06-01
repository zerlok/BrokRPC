from __future__ import annotations

import inspect
import typing as t

if t.TYPE_CHECKING:
    from concurrent.futures import Executor

from contextlib import asynccontextmanager
from dataclasses import replace

from brokrpc.abc import (
    BinaryConsumer,
    BinaryPublisher,
    BoundConsumer,
    Consumer,
    ConsumerMiddleware,
    Publisher,
    PublisherMiddleware,
    Serializer,
)
from brokrpc.consumer import AsyncFuncConsumer, DecodingConsumer, SyncFuncConsumer
from brokrpc.middleware import ConsumerMiddlewareWrapper, PublisherMiddlewareWrapper
from brokrpc.options import BindingOptions, ExchangeOptions, PublisherOptions, QueueOptions, merge_options
from brokrpc.publisher import EncodingPublisher


def ident[T](obj: T) -> T:
    return obj


class PublisherBuilder[U, V]:
    def __init__(
        self,
        provider: t.Callable[[PublisherOptions | None], t.AsyncContextManager[BinaryPublisher]],
        wrapper: t.Callable[[BinaryPublisher], Publisher[U, V]],
        exchange: ExchangeOptions | None = None,
    ) -> None:
        self.__provider = provider
        self.__wrapper = wrapper
        self.__exchange = exchange

    def set_exchange(self, exchange: ExchangeOptions | None) -> PublisherBuilder[U, V]:
        return PublisherBuilder(self.__provider, self.__wrapper, merge_options(self.__exchange, exchange))

    @t.overload
    def add_serializer(self, serializer: None) -> PublisherBuilder[U, V]: ...

    @t.overload
    def add_serializer[U2](self, serializer: t.Callable[[U2], U]) -> PublisherBuilder[U2, V]: ...

    @t.overload
    def add_serializer[U2](self, serializer: Serializer[U2, U]) -> PublisherBuilder[U2, V]: ...

    def add_serializer[U2](
        self,
        serializer: t.Callable[[U2], U] | Serializer[U2, U] | None,
    ) -> PublisherBuilder[U2, V]:
        if serializer is None:
            return t.cast(PublisherBuilder[U2, V], self)

        inner_wrapper = self.__wrapper
        dump_message = serializer.encode_message if isinstance(serializer, Serializer) else serializer

        def wrapper(inner: BinaryPublisher) -> Publisher[U2, V]:
            return EncodingPublisher(inner_wrapper(inner), dump_message)

        return PublisherBuilder(self.__provider, wrapper, self.__exchange)

    def add_middleware[U2, V2](
        self,
        middleware: PublisherMiddleware[Publisher[U, V], U2, V2],
    ) -> PublisherBuilder[U2, V2]:
        inner_wrapper = self.__wrapper

        def wrapper(inner: BinaryPublisher) -> Publisher[U2, V2]:
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
        binder: t.Callable[[BinaryConsumer, BindingOptions], t.AsyncContextManager[BoundConsumer]],
        wrapper: t.Callable[[Consumer[U, V]], BinaryConsumer],
        exchange: ExchangeOptions | None = None,
        queue: QueueOptions | None = None,
    ) -> None:
        self.__binder = binder
        self.__wrapper = wrapper
        self.__exchange = exchange
        self.__queue = queue

    def set_exchange(self, exchange: ExchangeOptions | None) -> ConsumerBuilder[U, V]:
        return ConsumerBuilder(self.__binder, self.__wrapper, merge_options(self.__exchange, exchange), self.__queue)

    def set_queue(self, queue: QueueOptions | None) -> ConsumerBuilder[U, V]:
        return ConsumerBuilder(self.__binder, self.__wrapper, self.__exchange, merge_options(self.__queue, queue))

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
        load_message = serializer.decode_message if isinstance(serializer, Serializer) else serializer

        def wrapper(inner: Consumer[U2, V]) -> BinaryConsumer:
            return inner_wrapper(DecodingConsumer(inner, load_message))

        return ConsumerBuilder(self.__binder, wrapper, self.__exchange, self.__queue)

    def add_middleware(
        self,
        middleware: ConsumerMiddleware[Consumer[U, V], U, V],
    ) -> ConsumerBuilder[U, V]:
        inner_wrapper = self.__wrapper

        def wrapper(inner: Consumer[U, V]) -> BinaryConsumer:
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
        inner: t.Callable[[U], t.Coroutine[object, object, V]],
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
        inner: Consumer[U, V] | t.Callable[[U], t.Coroutine[object, object, V]] | t.Callable[[U], V],
        options: BindingOptions,
        executor: Executor | None = None,
    ) -> t.AsyncContextManager[BoundConsumer]:
        consumer: Consumer[U, V]

        match inner:
            case Consumer():
                consumer = inner

            case async_func if inspect.iscoroutinefunction(async_func):
                consumer = AsyncFuncConsumer(async_func)

            case sync_func if callable(sync_func):
                consumer = SyncFuncConsumer(
                    # TODO: avoid cast
                    t.cast(t.Callable[[U], V], sync_func),
                    executor,
                )

            case _:
                t.assert_never(inner)

        return self.__binder(self.__wrapper(consumer), self.__clear_binding_options(options))

    def __clear_binding_options(self, binding: BindingOptions) -> BindingOptions:
        if self.__exchange is None and self.__queue is None:
            return binding

        return replace(
            binding,
            exchange=merge_options(self.__exchange, binding.exchange),
            queue=merge_options(self.__queue, binding.queue),
        )
