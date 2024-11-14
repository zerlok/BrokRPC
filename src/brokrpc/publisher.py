from __future__ import annotations

import typing as t

from brokrpc.abc import Publisher
from brokrpc.stringify import to_str_obj


class EncodingPublisher[A, V, B](Publisher[B, V]):
    def __init__(self, inner: Publisher[A, V], encoder: t.Callable[[B], A]) -> None:
        self.__inner = inner
        self.__encoder = encoder

    def __str__(self) -> str:
        return to_str_obj(self, inner=self.__inner)

    async def publish(self, message: B) -> V:
        encoded_message = self.__encoder(message)
        result = await self.__inner.publish(encoded_message)

        # NOTE: `result` var is for debugger.
        return result  # noqa: RET504


class DecodingPublisher[U, A, B](Publisher[U, B]):
    def __init__(self, inner: Publisher[U, A], decoder: t.Callable[[A], B]) -> None:
        self.__inner = inner
        self.__decoder = decoder

    def __str__(self) -> str:
        return to_str_obj(self, inner=self.__inner)

    async def publish(self, message: U) -> B:
        result = await self.__inner.publish(message)
        decoded_result = self.__decoder(result)

        # NOTE: `decoded_result` var is for debugger.
        return decoded_result  # noqa: RET504
