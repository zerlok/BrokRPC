import typing as t

from google.protobuf.any_pb2 import Any
from google.protobuf.message import DecodeError, EncodeError
from google.protobuf.message import Message as ProtobufMessage

from brokrpc.abc import Serializer
from brokrpc.message import BinaryMessage, Message, PackedMessage, UnpackedMessage
from brokrpc.model import SerializerDumpError, SerializerLoadError


class ProtobufSerializer[T: ProtobufMessage](Serializer[Message[T], Message[bytes]]):
    __CONTENT_TYPE: t.Final[str] = "application/protobuf"

    def __init__(self, message_type: type[T]) -> None:
        self.__message_type = message_type

    def dump_message(self, message: Message[T]) -> PackedMessage[bytes]:
        assert isinstance(message, self.__message_type)

        try:
            body = message.body.SerializeToString()

        except EncodeError as err:
            details = f"can't encode protobuf message: {err}"
            raise SerializerDumpError(details, message) from err

        return PackedMessage(
            original=message,
            body=body,
            content_type=self.__CONTENT_TYPE,
            content_encoding=None,
            message_type=message.body.DESCRIPTOR.full_name,
        )

    def load_message(self, message: BinaryMessage) -> UnpackedMessage[T]:
        if message.content_type != self.__CONTENT_TYPE:
            details = f"invalid content type: {message.content_type}"
            raise SerializerLoadError(details, message)

        if message.message_type != self.__message_type.DESCRIPTOR.full_name:
            details = f"invalid message type: {message.message_type}"
            raise SerializerLoadError(details, message)

        try:
            body = self.__message_type.FromString(message.body)

        except DecodeError as err:
            details = f"can't decode protobuf message: {err}"
            raise SerializerLoadError(details, message) from err

        return UnpackedMessage(
            original=message,
            body=body,
        )


def pack_any(msg: ProtobufMessage) -> Any:
    result = Any()
    result.Pack(msg)
    return result


def unpack_any[T: ProtobufMessage](msg_type: t.Type[T], value: Any) -> tuple[bool, T]:
    msg = msg_type()
    return value.Unpack(msg), msg
