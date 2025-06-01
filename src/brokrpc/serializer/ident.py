from brokrpc.abc import Serializer


class IdentSerializer[T](Serializer[T, T]):
    def encode_message(self, message: T) -> T:
        return message

    def decode_message(self, message: T) -> T:
        return message
