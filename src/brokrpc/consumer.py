from __future__ import annotations

import asyncio
import typing as t

if t.TYPE_CHECKING:
    from concurrent.futures import Executor

from brokrpc.abc import Consumer
from brokrpc.stringify import to_str_obj


class DecodingConsumer[A, V, B](Consumer[A, V]):
    def __init__(self, inner: Consumer[B, V], decode: t.Callable[[A], B]) -> None:
        self.__inner = inner
        self.__decode = decode

    def __str__(self) -> str:
        return to_str_obj(self, inner=self.__inner)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(inner={self.__inner!r}, decode={self.__decode!r})"

    async def consume(self, message: A) -> V:
        decoded_message = self.__decode(message)
        result = await self.__inner.consume(decoded_message)

        # NOTE: `result` var is for debugger.
        return result  # noqa: RET504


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

        # NOTE: `encoded_result` var is for debugger.
        return encoded_result  # noqa: RET504


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

        # NOTE: `result` var is for debugger.
        return result  # noqa: RET504


class AsyncFuncConsumer[U, V](Consumer[U, V]):
    def __init__(self, func: t.Callable[[U], t.Awaitable[V]]) -> None:
        self.__func = func

    def __str__(self) -> str:
        return to_str_obj(self, inner=self.__func)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(func={self.__func!r})"

    async def consume(self, message: U) -> V:
        result = await self.__func(message)

        # NOTE: `result` var is for debugger.
        return result  # noqa: RET504
