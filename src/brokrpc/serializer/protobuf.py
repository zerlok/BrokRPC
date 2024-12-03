import typing as t
from functools import cached_property

from google.protobuf.any_pb2 import Any
from google.protobuf.message import DecodeError, EncodeError
from google.protobuf.message import Message as ProtobufMessage

from brokrpc.abc import Serializer
from brokrpc.message import BinaryMessage, Message, PackedMessage, UnpackedMessage
from brokrpc.model import SerializerDumpError, SerializerLoadError
from brokrpc.rpc.abc import RPCSerializer
from brokrpc.rpc.model import BinaryRequest, BinaryResponse, Request, Response


class ProtobufSerializer[T: ProtobufMessage](Serializer[Message[T], Message[bytes]]):
    __CONTENT_TYPE: t.Final[str] = "application/protobuf"

    def __init__(
        self,
        message_type: type[T],
        *,
        strict: bool = False,
    ) -> None:
        self.__message_type = message_type
        self.__strict = strict

    def dump_message(self, message: Message[T]) -> PackedMessage[bytes]:
        assert isinstance(message.body, self.__message_type)

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
            message_type=self.__message_qualname,
        )

    def load_message(self, message: BinaryMessage) -> UnpackedMessage[T]:
        if message.content_type != self.__CONTENT_TYPE and (self.__strict or message.content_type is not None):
            details = f"invalid content type: {message.content_type}"
            raise SerializerLoadError(details, message)

        if message.message_type != self.__message_qualname and (self.__strict or message.message_type is not None):
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

    @cached_property
    def __message_qualname(self) -> str:
        return self.__message_type.DESCRIPTOR.full_name


class RPCProtobufSerializer[U: ProtobufMessage, V: ProtobufMessage](RPCSerializer[U, V]):
    def __init__(self, request_type: type[U], response_type: type[V], *, strict: bool = False) -> None:
        self.__request = ProtobufSerializer(request_type, strict=strict)
        self.__response = ProtobufSerializer(response_type, strict=strict)

    def dump_unary_request(self, request: Request[U]) -> BinaryRequest:
        return self.__request.dump_message(request)

    def load_unary_request(self, request: BinaryRequest) -> Request[U]:
        return self.__request.load_message(request)

    def dump_unary_response(self, response: Response[V]) -> BinaryResponse:
        return self.__response.dump_message(response)

    def load_unary_response(self, response: BinaryResponse) -> Response[V]:
        return self.__response.load_message(response)


def pack_any(msg: ProtobufMessage) -> Any:
    result = Any()
    result.Pack(msg)
    return result


def unpack_any[T: ProtobufMessage](msg_type: type[T], value: Any) -> tuple[bool, T]:
    msg = msg_type()
    return value.Unpack(msg), msg
