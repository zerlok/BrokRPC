import typing as t

from google.protobuf.any_pb2 import Any
from google.protobuf.message import DecodeError, EncodeError
from google.protobuf.message import Message as ProtobufMessage

from protomq.abc import Serializer
from protomq.exception import SerializerDumpError, SerializerLoadError
from protomq.model import Message, RawMessage
from protomq.options import MessageOptions


class ProtobufSerializer[T: ProtobufMessage](Serializer[Message[T], RawMessage]):
    __CONTENT_TYPE: t.Final[str] = "application/protobuf"

    def __init__(self, message_type: type[T]) -> None:
        self.__message_type = message_type

    def dump_message(self, message: Message[T]) -> RawMessage:
        assert isinstance(message, self.__message_type)

        try:
            body = message.body.SerializeToString()

        except EncodeError as err:
            details = f"can't encode protobuf message: {err}"
            raise SerializerDumpError(details, message) from err

        return message.repack(
            body,
            MessageOptions(
                content_type=self.__CONTENT_TYPE,
                content_encoding=None,
                message_type=message.body.DESCRIPTOR.full_name,
            ),
        )

    def load_message(self, message: RawMessage) -> Message[T]:
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

        return message.repack(body)


# class ProtobufRPCSerializer[U: ProtobufMessage, V: ProtobufMessage](RPCSerializer[U, V]):
#     def __init__(self, request_type: type[U], response_type: type[V]) -> None:
#         self.__request = ProtobufSerializer(request_type)
#         self.__response = ProtobufSerializer(response_type)
#
#     def dump_unary_request(self, request: Request[U]) -> Request[bytes]:
#         return self.__request.dump_message(request)
#
#     def load_unary_request(self, request: Request[bytes]) -> Request[U]:
#         return self.__request.load_message(request)
#
#     def dump_unary_response(self, response: Response[U, V]) -> RawMessage:
#         return self.__response.dump_message(response)
#
#     def load_unary_response(self, response: RawMessage) -> Message[V]:
#         return self.__response.load_message(response)


def pack_any(msg: ProtobufMessage) -> Any:
    result = Any()
    result.Pack(msg)
    return result


def unpack_any[T: ProtobufMessage](msg_type: t.Type[T], value: Any) -> tuple[bool, T]:
    msg = msg_type()
    return value.Unpack(msg), msg
