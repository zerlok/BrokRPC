import typing as t

from protomq.abc import Publisher


class EncodingPublisher[U, V, T](Publisher[T, V]):
    def __init__(self, inner: Publisher[U, V], encoder: t.Callable[[T], U]) -> None:
        self.__inner = inner
        self.__encoder = encoder

    async def publish(self, message: T) -> V:
        encoded_message = self.__encoder(message)
        result = await self.__inner.publish(encoded_message)

        return result


class DecodingPublisher[U, V, T](Publisher[U, T]):
    def __init__(self, inner: Publisher[U, V], decoder: t.Callable[[V], T]) -> None:
        self.__inner = inner
        self.__decoder = decoder

    async def publish(self, message: U) -> T:
        result = await self.__inner.publish(message)
        decoded_result = self.__decoder(result)

        return decoded_result
