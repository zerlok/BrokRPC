import asyncio
import typing as t

from brokrpc.abc import Consumer
from brokrpc.message import Message
from brokrpc.model import ConsumerResult
from brokrpc.rpc.abc import UnaryUnaryHandler
from brokrpc.rpc.model import Request
from tests.stub.proto.greeting_pb2 import GreetingRequest, GreetingResponse

type ReceiveWaiterConsumer = (
    Consumer[Message[object], ConsumerResult]
    | t.Callable[[Message[object]], t.Awaitable[ConsumerResult]]
    | t.Callable[[Message[object]], ConsumerResult]
)
type GreetingHandler = (
    UnaryUnaryHandler[Request[GreetingRequest], GreetingResponse]
    | t.Callable[[Request[GreetingRequest]], t.Awaitable[GreetingResponse]]
    | t.Callable[[Request[GreetingRequest]], GreetingResponse]
)


class ReceiveWaiter:
    class ObjConsumer(Consumer[Message[object], ConsumerResult]):
        def __init__(self, fut: asyncio.Future[Message[object]]) -> None:
            self.fut = fut

        async def consume(self, message: Message[object]) -> ConsumerResult:
            await asyncio.sleep(0)
            self.fut.set_result(message)
            return None

    class GreetingHandler(UnaryUnaryHandler[Request[GreetingRequest], GreetingResponse]):
        def __init__(self, fut: asyncio.Future[Request[object]], processor: t.Callable[[str], str]) -> None:
            self.fut = fut
            self.__processor = processor

        async def handle(self, request: Request[GreetingRequest]) -> GreetingResponse:
            self.fut.set_result(request)
            return GreetingResponse(result=self.__processor(request.body.name))

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

    def handler_sync(self) -> t.Callable[[Request[GreetingRequest]], GreetingResponse]:
        def handle(message: Request[GreetingRequest]) -> GreetingResponse:
            self.fut.set_result(message)
            return GreetingResponse(result=self.process_value(message.body.name))

        return handle

    def handler_async(self) -> t.Callable[[Request[GreetingRequest]], t.Awaitable[GreetingResponse]]:
        async def handle(message: Request[GreetingRequest]) -> GreetingResponse:
            await asyncio.sleep(0)
            self.fut.set_result(message)
            return GreetingResponse(result=self.process_value(message.body.name))

        return handle

    def handler_impl(self) -> UnaryUnaryHandler[Request[GreetingRequest], GreetingResponse]:
        return self.GreetingHandler(self.fut, self.process_value)

    def process_value(self, name: str) -> str:
        return f"I greet you, {name}!"

    async def wait(self) -> Message[object]:
        return await self.fut
