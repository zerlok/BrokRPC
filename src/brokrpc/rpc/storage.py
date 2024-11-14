from __future__ import annotations

import asyncio
import typing as t
import uuid
from contextlib import contextmanager


class WaiterStorage[T]:
    @classmethod
    @contextmanager
    def create(cls) -> t.Iterator[WaiterStorage[T]]:
        waiters: dict[uuid.UUID, asyncio.Future[T]] = {}
        try:
            yield cls(waiters)

        finally:
            for fut in waiters.values():
                if not fut.done():
                    fut.cancel()

            waiters.clear()

    def __init__(self, waiters: dict[uuid.UUID, asyncio.Future[T]]) -> None:
        self.__waiters = waiters

    @contextmanager
    def context(self) -> t.Iterator[tuple[str, asyncio.Future[T]]]:
        key = uuid.uuid4()
        fut = self.__waiters[key] = asyncio.Future[T]()

        try:
            yield key.hex, fut

        finally:
            if not fut.done():
                fut.cancel()

            self.__waiters.pop(key, None)

    def set_waiter(self, key: str, value: T | Exception) -> None:
        if isinstance(value, Exception):
            self.set_exception(key, value)

        else:
            self.set_result(key, value)

    def set_result(self, key: str, value: T) -> None:
        self.__waiters[uuid.UUID(hex=key)].set_result(value)

    def set_exception(self, key: str, value: Exception) -> None:
        self.__waiters[uuid.UUID(hex=key)].set_exception(value)

    def cancel(self, key: str, reason: str | None) -> None:
        self.__waiters[uuid.UUID(hex=key)].cancel(reason)
