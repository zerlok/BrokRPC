import asyncio
import inspect
import typing as t
from concurrent.futures import Executor
from functools import wraps

from brokrpc.rpc.abc import StreamStreamFunc, StreamUnaryFunc, UnaryStreamFunc, UnaryUnaryFunc


class Sync2AsyncNormalizer:
    def __init__(self, executor: Executor | None) -> None:
        self.__executor = executor

    def normalize_unary_unary[U, V](self, func: t.Callable[[U], V] | UnaryUnaryFunc[U, V]) -> UnaryUnaryFunc[U, V]:
        if inspect.iscoroutinefunction(func):
            assert not inspect.isasyncgenfunction(func)

            return t.cast(UnaryUnaryFunc[U, V], func)

        return self.__to_unary_unary(t.cast(t.Callable[[U], V], func))

    def normalize_unary_stream[U, V](
        self,
        func: t.Callable[[U], t.Iterable[V]] | UnaryStreamFunc[U, V],
    ) -> UnaryStreamFunc[U, V]:
        if inspect.isasyncgenfunction(func):
            return t.cast(UnaryStreamFunc[U, V], func)

        assert not inspect.iscoroutinefunction(func)

        return self.__to_unary_stream(t.cast(t.Callable[[U], t.Iterable[V]], func))

    def normalize_stream_unary[U, V](
        self,
        func: t.Callable[[t.Iterable[U]], V] | StreamUnaryFunc[U, V],
    ) -> StreamUnaryFunc[U, V]:
        raise NotImplementedError

    def normalize_stream_stream[U, V](
        self,
        func: t.Callable[[t.Iterable[U]], t.Iterable[V]] | StreamStreamFunc[U, V],
    ) -> StreamStreamFunc[U, V]:
        raise NotImplementedError

    def __to_unary_unary[U, V](self, func: t.Callable[[U], V]) -> UnaryUnaryFunc[U, V]:
        @wraps(func)
        async def do_async(message: U) -> V:
            return await asyncio.get_event_loop().run_in_executor(self.__executor, func, message)

        return t.cast(UnaryUnaryFunc[U, V], do_async)

    def __to_unary_stream[U, V](self, func: t.Callable[[U], t.Iterable[V]]) -> UnaryStreamFunc[U, V]:
        @wraps(func)
        async def do_async(message: U) -> t.AsyncIterable[V]:
            loop = asyncio.get_event_loop()

            iterable = await loop.run_in_executor(self.__executor, func, message)
            iterator = await loop.run_in_executor(self.__executor, iterable.__iter__)

            while True:
                try:
                    value = await loop.run_in_executor(self.__executor, iterator.__next__)
                except StopIteration:
                    break

                yield value

        return t.cast(UnaryStreamFunc[U, V], do_async)
