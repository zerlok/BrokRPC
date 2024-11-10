import asyncio

from protomq.abc import Consumer
from protomq.model import ConsumerResult, RawMessage


class SimpleConsumer(Consumer[RawMessage, ConsumerResult]):
    def __init__(self) -> None:
        self.__fut: asyncio.Future[RawMessage] = asyncio.Future()

    async def consume(self, message: RawMessage) -> ConsumerResult:
        self.__fut.set_result(message)
        return None

    async def wait(self) -> RawMessage:
        return await self.__fut
