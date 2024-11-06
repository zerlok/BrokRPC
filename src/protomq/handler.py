from __future__ import annotations

import asyncio
import typing as t
from concurrent.futures import Executor

from protomq.abc import Consumer, Publisher, Serializer
from protomq.message import ConsumerReject, ConsumerResult, Message, RawMessage
from protomq.options import MessageOptions


class SyncFuncHandler[U, V](Consumer[RawMessage, ConsumerResult]):
    def __init__(
        self,
        func: t.Callable[[U], V],
        serializer: Serializer[U, RawMessage],
        replier: Publisher[Message[V], ConsumerResult],
        executor: Executor | None = None,
    ) -> None:
        self.__func = func
        self.__serializer = serializer
        self.__replier = replier
        self.__executor = executor

    async def consume(self, request_message: RawMessage) -> ConsumerResult:
        reply_options = get_reply_options(request_message)
        if reply_options is None:
            return reject_request_no_reply(request_message)

        response_message = await asyncio.get_running_loop().run_in_executor(
            self.__executor,
            self.__handle,
            request_message,
            reply_options,
        )
        result = await self.__replier.publish(response_message)

        return result

    def __handle(self, request_message: RawMessage, reply_options: MessageOptions) -> Message[V]:
        request_payload = self.__serializer.load_message(request_message)
        response_payload = self.__func(request_payload)
        response_message = Message.from_options(response_payload, reply_options)

        return response_message


class AsyncFuncHandler[U, V](Consumer[RawMessage, ConsumerResult]):
    def __init__(
        self,
        func: t.Callable[[U], t.Awaitable[V]],
        serializer: Serializer[U, RawMessage],
        replier: Publisher[Message[V], ConsumerResult],
    ) -> None:
        self.__func = func
        self.__serializer = serializer
        self.__replier = replier

    async def consume(self, request_message: RawMessage) -> ConsumerResult:
        reply_options = get_reply_options(request_message)
        if reply_options is None:
            return reject_request_no_reply(request_message)

        request_payload = self.__serializer.load_message(request_message)
        response_payload = await self.__func(request_payload)
        response_message = Message.from_options(response_payload, reply_options)
        result = await self.__replier.publish(response_message)

        return result


def get_reply_options(request_message: Message[t.Any]) -> MessageOptions | None:
    return (
        MessageOptions(
            exchange=request_message.exchange,
            routing_key=request_message.reply_to,
            correlation_id=request_message.correlation_id,
        )
        if request_message.reply_to is not None and request_message.correlation_id is not None
        else None
    )


def reject_request_no_reply(request_message: Message[t.Any]) -> ConsumerResult:
    return ConsumerReject(reason=f"can't get reply options from request message: {request_message}")
