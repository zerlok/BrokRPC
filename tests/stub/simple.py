import asyncio

from protomq.abc import Consumer
from protomq.message import BinaryMessage
from protomq.model import ConsumerResult


class SimpleConsumer(Consumer[BinaryMessage, ConsumerResult]):
    def __init__(self) -> None:
        self.__fut: asyncio.Future[BinaryMessage] = asyncio.Future()

    async def consume(self, message: BinaryMessage) -> ConsumerResult:
        self.__fut.set_result(message)
        return None

    async def wait(self) -> BinaryMessage:
        return await self.__fut
