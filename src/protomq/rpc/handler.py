from __future__ import annotations

import asyncio
import typing as t
from concurrent.futures import Executor

from protomq.abc import Consumer, Publisher
from protomq.model import ConsumerAck, ConsumerReject, ConsumerResult, Message, PublisherResult, RawMessage
from protomq.rpc.abc import HandlerSerializer
from protomq.rpc.model import Response
from protomq.rpc.storage import WaiterStorage


class SyncFuncHandler[U, V](Consumer[RawMessage, ConsumerResult]):
    def __init__(
        self,
        func: t.Callable[[U], V],
        serializer: HandlerSerializer[U, V],
        replier: Publisher[RawMessage, PublisherResult],
        executor: Executor | None = None,
    ) -> None:
        self.__func = func
        self.__serializer = serializer
        self.__replier = replier
        self.__executor = executor

    async def consume(self, request_message: RawMessage) -> ConsumerResult:
        handle_result, response_message = await asyncio.get_running_loop().run_in_executor(
            self.__executor,
            self.__handle,
            request_message,
        )

        if isinstance(handle_result, ConsumerAck):
            response_result = await self.__replier.publish(response_message)

        return handle_result

    def __handle(self, request_message: RawMessage) -> tuple[ConsumerResult, RawMessage]:
        response_factory = get_response_factory(request_message, self.__serializer)
        if response_factory is None:
            return reject_request_no_reply(request_message), request_message

        request_payload = self.__serializer.load_unary_request(request_message)
        response_payload = self.__func(request_payload)
        response_message = response_factory(request_payload, response_payload)

        return ConsumerAck(), response_message


class AsyncFuncHandler[U, V](Consumer[RawMessage, ConsumerResult]):
    def __init__(
        self,
        func: t.Callable[[U], t.Awaitable[V]],
        serializer: HandlerSerializer[U, V],
        replier: Publisher[RawMessage, PublisherResult],
    ) -> None:
        self.__func = func
        self.__serializer = serializer
        self.__replier = replier

    async def consume(self, request_message: RawMessage) -> ConsumerResult:
        response_factory = get_response_factory(request_message, self.__serializer)
        if response_factory is None:
            return reject_request_no_reply(request_message)

        request_payload = self.__serializer.load_unary_request(request_message)
        response_payload = await self.__func(request_payload)
        response_message = response_factory(request_payload, response_payload)
        response_result = await self.__replier.publish(response_message)

        return ConsumerAck()


class HandlerResponseConsumer[T](Consumer[RawMessage, ConsumerResult]):
    def __init__(self, serializer: t.Callable[[RawMessage], T], storage: WaiterStorage[T]) -> None:
        self.__serializer = serializer
        self.__storage = storage

    async def consume(self, response_message: RawMessage) -> ConsumerResult:
        if response_message.correlation_id is None:
            return ConsumerReject(reason=f"can't get correlation id from response message: {response_message}")

        try:
            response_payload = self.__serializer(response_message)

        except Exception as err:
            response_payload = err

        self.__storage.set_waiter(response_message.correlation_id, response_payload)

        return ConsumerAck()


def get_response_factory[U, V](
    request_message: RawMessage,
    serializer: HandlerSerializer[U, V],
) -> t.Callable[[U, V], RawMessage] | None:
    if (reply_to := request_message.reply_to) is not None and (
        correlation_id := request_message.correlation_id
    ) is not None:

        def create(request_payload: U, response_payload: V) -> RawMessage:
            reply = Response(
                to_request=request_payload,
                body=response_payload,
                routing_key=reply_to,
                exchange=request_message.exchange,
                correlation_id=correlation_id,
                timeout=request_message.timeout,
            )
            response_message = serializer.dump_unary_response(reply)

            return response_message

        return create

    return None


def reject_request_no_reply(request_message: Message[object]) -> ConsumerResult:
    return ConsumerReject(reason=f"can't get reply options from request message: {request_message}")
