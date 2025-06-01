from __future__ import annotations

import asyncio
import typing as t

if t.TYPE_CHECKING:
    from datetime import timedelta

    from brokrpc.abc import BinaryPublisher
    from brokrpc.options import ExchangeOptions
    from brokrpc.rpc.model import BinaryRequest, Request, Response
    from brokrpc.rpc.storage import WaiterStorage


from brokrpc.message import create_message
from brokrpc.rpc.abc import Caller
from brokrpc.stringify import to_str_obj


class RequestCaller[U, V](Caller[U, V]):
    # TODO: shorter arguments
    def __init__(  # noqa: PLR0913
        self,
        requester: BinaryPublisher,
        routing_key: str,
        serializer: t.Callable[[Request[U]], BinaryRequest],
        reply_to: str,
        storage: WaiterStorage[Response[V]],
        exchange: ExchangeOptions | None = None,
        timeout: timedelta | None = None,
    ) -> None:
        self.__requester = requester
        self.__routing_key = routing_key
        self.__serializer = serializer
        self.__reply_to = reply_to
        self.__storage = storage
        self.__exchange = exchange
        self.__timeout = timeout

    def __str__(self) -> str:
        return to_str_obj(self, requester=self.__requester, routing_key=self.__routing_key, reply_to=self.__reply_to)

    async def invoke(self, request_payload: U) -> Response[V]:
        waiter: asyncio.Future[Response[V]]

        with self.__storage.context() as (correlation_id, waiter):
            request_message = self.__serializer(
                create_message(
                    body=request_payload,
                    exchange=self.__exchange.name if self.__exchange is not None else None,
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
