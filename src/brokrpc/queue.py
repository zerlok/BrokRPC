from __future__ import annotations

import abc
import asyncio
import typing as t
from collections import deque
from contextlib import suppress

from brokrpc.stringify import to_str_obj


class Store[T](t.Collection[T], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def __iter__(self) -> t.Iterator[T]:
        raise NotImplementedError

    @abc.abstractmethod
    def __contains__(self, x: object, /) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def push(self, item: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def pop(self) -> T:
        raise NotImplementedError

    @abc.abstractmethod
    def remove(self, item: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def clear(self) -> None:
        raise NotImplementedError


class DequeStore[T](Store[T]):
    __slots__ = ("__deque",)

    def __init__(self, items: t.Sequence[T] | None = None, max_size: int | None = None) -> None:
        self.__deque = deque[T](items or (), maxlen=max_size)

    def __str__(self) -> str:
        return str(self.__deque)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(items={self.__deque!r}, max_size={self.__deque.maxlen!r})"

    def __len__(self) -> int:
        return len(self.__deque)

    def __iter__(self) -> t.Iterator[T]:
        return iter(self.__deque)

    def __contains__(self, x: object, /) -> bool:
        return x in self.__deque

    def push(self, item: T) -> None:
        self.__deque.append(item)

    def pop(self) -> T:
        return self.__deque.popleft()

    def remove(self, item: T) -> None:
        with suppress(ValueError):
            self.__deque.remove(item)

    def clear(self) -> None:
        self.__deque.clear()

    @property
    def is_empty(self) -> bool:
        return len(self.__deque) == 0

    @property
    def is_full(self) -> bool:
        return self.__deque.maxlen is not None and len(self.__deque) >= self.__deque.maxlen

    @property
    def max_len(self) -> int | None:
        return self.__deque.maxlen


class Waiters[T]:
    __slots__ = ("__store",)

    def __init__(self, store: Store[asyncio.Future[T]]) -> None:
        self.__store = store

    def __str__(self) -> str:
        return to_str_obj(self, len=len(self))

    def __len__(self) -> int:
        return len(self.__store)

    def wakeup(self, result: T, n: int = 1) -> int:
        i = 0

        while self.__store and i < n:
            fut = self.__store.pop()
            if not fut.done():
                fut.set_result(result)
                i += 1

        return i

    def wakeup_all(self, result: T) -> int:
        return self.wakeup(result, len(self.__store))

    async def wait(self) -> T:
        fut = asyncio.Future[T]()

        self.__store.push(fut)
        try:
            result = await fut

        except BaseException:
            self.__store.remove(fut)
            raise

        else:
            return result


class Queue[T]:
    __slots__ = ("__closed", "__done", "__items", "__reads", "__writes")

    def __init__(
        self,
        items: DequeStore[T] | t.Sequence[T] | None = None,
        max_size: int | None = None,
    ) -> None:
        self.__items = items if isinstance(items, DequeStore) else DequeStore[T](items, max_size)

        self.__closed = False
        self.__reads = Waiters(DequeStore[asyncio.Future[None]]())
        self.__writes = Waiters(DequeStore[asyncio.Future[None]]())
        self.__done = asyncio.Event()

    def __str__(self) -> str:
        return to_str_obj(
            self,
            len=len(self),
            max=self.__items.max_len,
            reads=len(self.__reads),
            writes=len(self.__writes),
            closed=self.is_closed,
            done=self.is_done,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(items={list(self.__items)!r}, max_size={self.__items.max_len!r})"

    def __len__(self) -> int:
        return len(self.__items)

    @property
    def is_empty(self) -> bool:
        return self.__items.is_empty

    @property
    def is_full(self) -> bool:
        return self.__items.is_full

    @property
    def is_closed(self) -> bool:
        return self.__closed

    @property
    def is_done(self) -> bool:
        return self.__done.is_set()

    async def read(self) -> t.AsyncIterator[T]:
        try:
            while not self.is_closed or not self.is_empty:
                if self.is_empty:
                    await self.__reads.wait()
                    if self.is_empty:
                        continue

                item = self.__items.pop()
                self.__writes.wakeup(None)

                yield item

        finally:
            # NOTE: this block may be executed after exception propagation, so `is_closed` may be False
            if self.is_closed and self.is_empty:  # type: ignore[redundant-expr]
                self.__done.set()

    async def write(self, *items: T) -> int:
        i = 0

        for item in items:
            if self.is_closed:
                break

            if self.is_full:
                await self.__writes.wait()
                if self.is_closed:
                    # NOTE: the queue may close while waiting for write
                    break  # type: ignore[unreachable]

            self.__items.push(item)

            self.__reads.wakeup(None)
            self.__done.clear()
            i += 1

        return i

    async def close(self, *, force: bool = False) -> None:
        if self.__closed:
            return

        self.__closed = True
        self.__writes.wakeup_all(None)
        reads = self.__reads.wakeup_all(None)

        if force:
            self.__items.clear()
            self.__done.set()

        elif reads > 0:
            await self.__done.wait()

        elif reads <= 0:
            self.__done.set()
