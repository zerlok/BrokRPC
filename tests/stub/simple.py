import asyncio
import typing as t

from brokrpc.abc import Consumer
from brokrpc.message import Message
from brokrpc.model import ConsumerResult
from brokrpc.rpc.model import Request

type ReceiveWaiterConsumer = (
    Consumer[Message[object], ConsumerResult]
    | t.Callable[[Message[object]], t.Awaitable[ConsumerResult]]
    | t.Callable[[Message[object]], ConsumerResult]
)
type ReceiveWaiterHandler = t.Callable[[Request[object]], t.Awaitable[str]] | t.Callable[[Request[object]], str]


class ReceiveWaiter:
    class ObjConsumer(Consumer[Message[object], ConsumerResult]):
        def __init__(self, fut: asyncio.Future[Message[object]]) -> None:
            self.fut = fut

        async def consume(self, message: Message[object]) -> ConsumerResult:
            self.fut.set_result(message)
            return None

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.fut: asyncio.Future[Message[object]] = asyncio.Future(loop=loop)

    def consumer_sync(self) -> t.Callable[[Message[object]], ConsumerResult]:
        def consume(message: Message[object]) -> ConsumerResult:
            self.fut.set_result(message)
            return None

        return consume

    def consumer_async(self) -> t.Callable[[Message[object]], t.Awaitable[ConsumerResult]]:
        async def consume(message: Message[object]) -> ConsumerResult:
            await asyncio.sleep(0)
            self.fut.set_result(message)
            return None

        return consume

    def consumer_impl(self) -> Consumer[Message[object], ConsumerResult]:
        return self.ObjConsumer(self.fut)

    def handler_sync(self) -> t.Callable[[Request[object]], str]:
        def handle(message: Request[object]) -> str:
            self.fut.set_result(message)
            return self.process_value(message.body)

        return handle

    def handler_async(self) -> t.Callable[[Request[object]], t.Awaitable[str]]:
        async def handle(message: Request[object]) -> str:
            await asyncio.sleep(0)
            self.fut.set_result(message)
            return self.process_value(message.body)

        return handle

    def handler_impl(self) -> object:
        raise NotImplementedError

    def process_value(self, value: object) -> str:
        return f"value={value}"

    async def wait(self) -> Message[object]:
        return await self.fut
