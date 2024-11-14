from brokrpc.abc import Serializer


class IdentSerializer[T](Serializer[T, T]):
    def dump_message(self, message: T) -> T:
        return message

    def load_message(self, message: T) -> T:
        return message
