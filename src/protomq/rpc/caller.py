from __future__ import annotations

import asyncio
import typing as t
from datetime import timedelta

from protomq.abc import BinaryPublisher
from protomq.message import AppMessage
from protomq.rpc.abc import Caller
from protomq.rpc.model import BinaryRequest, Request
from protomq.rpc.storage import WaiterStorage
from protomq.stringify import to_str_obj


class RequestCaller[U, V](Caller[U, V]):
    def __init__(
        self,
        requester: BinaryPublisher,
        routing_key: str,
        serializer: t.Callable[[Request[U]], BinaryRequest],
        reply_to: str,
        storage: WaiterStorage[V],
        timeout: timedelta | None,
    ) -> None:
        self.__requester = requester
        self.__routing_key = routing_key
        self.__serializer = serializer
        self.__reply_to = reply_to
        self.__storage = storage
        self.__timeout = timeout

    def __str__(self) -> str:
        return to_str_obj(self, requester=self.__requester, routing_key=self.__routing_key, reply_to=self.__reply_to)

    async def invoke(self, request_payload: U) -> V:
        waiter: asyncio.Future[V]

        with self.__storage.context() as (correlation_id, waiter):
            request_message = self.__serializer(
                AppMessage(
                    body=request_payload,
                    routing_key=self.__routing_key,
                    correlation_id=correlation_id,
                    reply_to=self.__reply_to,
                    timeout=self.__timeout,
                )
            )

            async with asyncio.timeout(
                request_message.timeout.total_seconds() if request_message.timeout is not None else None
            ):
                publish_result = await self.__requester.publish(request_message)
                if publish_result is False:
                    # TODO: raise appropriate exception
                    raise RuntimeError(...)

                response = await waiter

        return response
