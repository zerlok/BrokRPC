from __future__ import annotations

import asyncio
import typing as t
from concurrent.futures import Executor

from protomq.abc import Consumer
from protomq.stringify import to_str_obj


class DecodingConsumer[A, V, B](Consumer[A, V]):
    def __init__(self, inner: Consumer[B, V], decoder: t.Callable[[A], B]) -> None:
        self.__inner = inner
        self.__decoder = decoder

    def __str__(self) -> str:
        return to_str_obj(self, inner=self.__inner)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(inner={self.__inner!r}, decoder={self.__decoder!r})"

    async def consume(self, message: A) -> V:
        decoded_message = self.__decoder(message)
        result = await self.__inner.consume(decoded_message)

        return result


class EncodingConsumer[U, A, B](Consumer[U, A]):
    def __init__(self, inner: Consumer[U, B], encode: t.Callable[[B], A]) -> None:
        self.__inner = inner
        self.__encode = encode

    def __str__(self) -> str:
        return to_str_obj(self, inner=self.__inner)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(inner={self.__inner!r}, encode={self.__encode!r})"

    async def consume(self, message: U) -> A:
        result = await self.__inner.consume(message)
        encoded_result = self.__encode(result)

        return encoded_result


class SyncFuncConsumer[U, V](Consumer[U, V]):
    def __init__(self, func: t.Callable[[U], V], executor: Executor | None = None) -> None:
        self.__func = func
        self.__executor = executor

    def __str__(self) -> str:
        return to_str_obj(self, inner=self.__func)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(func={self.__func!r}, executor={self.__executor!r})"

    async def consume(self, message: U) -> V:
        result = await asyncio.get_running_loop().run_in_executor(self.__executor, self.__func, message)

        return result


class AsyncFuncConsumer[U, V](Consumer[U, V]):
    def __init__(self, func: t.Callable[[U], t.Awaitable[V]]) -> None:
        self.__func = func

    def __str__(self) -> str:
        return to_str_obj(self, inner=self.__func)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(func={self.__func!r})"

    async def consume(self, message: U) -> V:
        result = await self.__func(message)

        return result
