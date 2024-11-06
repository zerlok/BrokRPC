from __future__ import annotations

import typing as t
from dataclasses import replace

from protomq.abc import Publisher, Serializer
from protomq.message import PublisherResult, RawMessage
from protomq.rpc.storage import WaiterStorage


class EncodingPublisher[A, V, B](Publisher[B, V]):
    def __init__(self, inner: Publisher[A, V], encoder: t.Callable[[B], A]) -> None:
        self.__inner = inner
        self.__encoder = encoder

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}: {self.__inner}>"

    async def publish(self, message: B) -> V:
        encoded_message = self.__encoder(message)
        result = await self.__inner.publish(encoded_message)

        return result


class DecodingPublisher[U, A, B](Publisher[U, B]):
    def __init__(self, inner: Publisher[U, A], decoder: t.Callable[[A], B]) -> None:
        self.__inner = inner
        self.__decoder = decoder

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}: {self.__inner}>"

    async def publish(self, message: U) -> B:
        result = await self.__inner.publish(message)
        decoded_result = self.__decoder(result)

        return decoded_result


class RPCSender[U, V](Publisher[U, V]):
    def __init__(
        self,
        inner: Publisher[RawMessage, PublisherResult],
        serializer: Serializer[U, RawMessage],
        storage: WaiterStorage[V],
        reply_to: str,
    ) -> None:
        self.__inner = inner
        self.__serializer = serializer
        self.__storage = storage
        self.__reply_to = reply_to

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}: inner={self.__inner}; reply_to={self.__reply_to}>"

    async def publish(self, request_payload: U) -> V:
        request_message = self.__serializer.dump_message(request_payload)

        with self.__storage.context() as (correlation_id, waiter):
            await self.__inner.publish(
                replace(request_message, correlation_id=correlation_id, reply_to=self.__reply_to)
            )
            response = await waiter

        return response
