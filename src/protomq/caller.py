from __future__ import annotations

import typing as t
from dataclasses import replace

if t.TYPE_CHECKING:
    from protomq.abc import Publisher, Serializer
    from protomq.options import MessageOptions
    from protomq.storage import WaiterStorage


class UnaryUnaryCaller[U, V]:
    def __init__(
        self,
        requester: Publisher,
        serializer: Serializer[U, V],
        options: MessageOptions,
        waiters: WaiterStorage[V],
    ) -> None:
        self.__requester = requester
        self.__serializer = serializer
        self.__options = options
        self.__waiters = waiters

    async def request(self, payload: U) -> V:
        with self.__waiters.context() as (correlation_id, response):
            request = self.__serializer.dump_request(
                request=payload,
                options=replace(self.__options, correlation_id=correlation_id),
            )
            await self.__requester.publish(request)

            return await response
