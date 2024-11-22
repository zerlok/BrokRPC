import asyncio
import typing as t
from collections import deque


class InMemoryStream(t.AsyncIterator[bytes]):
    def __init__(self) -> None:
        self.__condition = asyncio.Condition()
        self.__buffer = deque[bytes]()
        self.__closed = False

    def __aiter__(self) -> t.AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        async with self.__condition:
            await self.__condition.wait_for(lambda: len(self.__buffer) > 0 or self.__closed)

            if len(self.__buffer) == 0 and self.__closed:
                raise StopAsyncIteration

            assert len(self.__buffer) > 0
            return self.__buffer.popleft()

    async def readlines(self) -> t.Sequence[bytes]:
        async with self.__condition:
            buffer = list(self.__buffer)
            self.__buffer.clear()
            self.__condition.notify_all()

            return buffer

    async def write(self, line: bytes, /) -> int:
        self.__buffer.append(line.strip())
        return len(line)

    async def flush(self) -> None:
        async with self.__condition:
            self.__condition.notify_all()

    async def close(self) -> None:
        async with self.__condition:
            self.__closed = True
            self.__condition.notify_all()
