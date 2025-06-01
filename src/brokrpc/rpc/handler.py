from __future__ import annotations

import asyncio
import typing as t

if t.TYPE_CHECKING:
    from concurrent.futures import Executor

    from brokrpc.rpc.abc import HandlerSerializer
    from brokrpc.rpc.model import BinaryResponse, Request
    from brokrpc.rpc.storage import WaiterStorage

from brokrpc.abc import Consumer, Publisher
from brokrpc.message import BinaryMessage, Message, create_message
from brokrpc.model import ConsumerAck, ConsumerReject, ConsumerResult, PublisherResult


class SyncFuncHandler[U, V](Consumer[BinaryMessage, ConsumerResult]):
    def __init__(
        self,
        func: t.Callable[[Request[U]], V],
        serializer: HandlerSerializer[U, V],
        replier: Publisher[BinaryMessage, PublisherResult],
        executor: Executor | None = None,
    ) -> None:
        self.__func = func
        self.__serializer = serializer
        self.__replier = replier
        self.__executor = executor

    async def consume(self, request_message: BinaryMessage) -> ConsumerResult:
        handle_result, response_message = await asyncio.get_running_loop().run_in_executor(
            self.__executor,
            self.__handle,
            request_message,
        )

        if isinstance(handle_result, ConsumerAck):
            await self.__replier.publish(response_message)

        return handle_result

    def __handle(self, request_message: BinaryMessage) -> tuple[ConsumerResult, BinaryMessage]:
        response_factory = get_response_factory(request_message, self.__serializer)
        if response_factory is None:
            return reject_request_no_reply(request_message), request_message

        request_payload = self.__serializer.load_unary_request(request_message)
        response_payload = self.__func(request_payload)
        response_message = response_factory(response_payload)

        return ConsumerAck(), response_message


class AsyncFuncHandler[U, V](Consumer[BinaryMessage, ConsumerResult]):
    def __init__(
        self,
        func: t.Callable[[Message[U]], t.Awaitable[V]],
        serializer: HandlerSerializer[U, V],
        replier: Publisher[BinaryMessage, PublisherResult],
    ) -> None:
        self.__func = func
        self.__serializer = serializer
        self.__replier = replier

    async def consume(self, request_message: BinaryMessage) -> ConsumerResult:
        response_factory = get_response_factory(request_message, self.__serializer)
        if response_factory is None:
            return reject_request_no_reply(request_message)

        request_payload = self.__serializer.load_unary_request(request_message)
        response_payload = await self.__func(request_payload)
        response_message = response_factory(response_payload)
        await self.__replier.publish(response_message)

        return ConsumerAck()


class HandlerResponseConsumer[T](Consumer[BinaryMessage, ConsumerResult]):
    def __init__(self, serializer: t.Callable[[BinaryMessage], T], storage: WaiterStorage[T]) -> None:
        self.__serializer = serializer
        self.__storage = storage

    async def consume(self, response_message: BinaryMessage) -> ConsumerResult:
        if response_message.correlation_id is None:
            return ConsumerReject(reason=f"can't get correlation id from response message: {response_message}")

        result: T | Exception
        try:
            result = self.__serializer(response_message)

        # NOTE: caught exception is propagated via storage waiter
        except Exception as err:  # noqa: BLE001
            result = err

        self.__storage.set_waiter(response_message.correlation_id, result)

        return ConsumerAck()


def get_response_factory[U, V](
    request: Request[U],
    serializer: HandlerSerializer[U, V],
) -> t.Callable[[V], BinaryResponse] | None:
    if (reply_to := request.reply_to) is not None and (correlation_id := request.correlation_id) is not None:

        def create(response_payload: V) -> BinaryResponse:
            reply = create_message(
                body=response_payload,
                routing_key=reply_to,
                exchange=request.exchange,
                correlation_id=correlation_id,
                timeout=request.timeout,
            )
            response_message = serializer.dump_unary_response(reply)

            # NOTE: `response_message` var is for debugger.
            return response_message  # noqa: RET504

        return create

    return None


def reject_request_no_reply(request_message: Message[object]) -> ConsumerResult:
    return ConsumerReject(reason=f"can't get reply options from request message: {request_message}")
