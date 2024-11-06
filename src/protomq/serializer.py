import typing as t
from dataclasses import replace
from json import JSONDecodeError, JSONDecoder, JSONEncoder

from google.protobuf.any_pb2 import Any
from google.protobuf.message import DecodeError, EncodeError
from google.protobuf.message import Message as ProtobufMessage

from protomq.abc import Serializer
from protomq.exception import SerializerDumpError, SerializerLoadError
from protomq.message import Message, RawMessage


class IdentSerializer[T](Serializer[T, T]):
    def dump_message(self, message: T) -> T:
        return message

    def load_message(self, message: T) -> T:
        return message


class JSONSerializer(Serializer[Message[object], RawMessage]):
    __CONTENT_TYPE: t.Final[str] = "application/json"

    def __init__(
        self,
        encoder: JSONEncoder | None = None,
        decoder: JSONDecoder | None = None,
        encoding: str | None = None,
    ) -> None:
        self.__encoder = encoder if encoder is not None else JSONEncoder(indent=None, separators=(",", ":"))
        self.__decoder = decoder if decoder is not None else JSONDecoder()
        self.__encoding = encoding if encoding is not None else "utf-8"

    def dump_message(self, message: Message[object]) -> RawMessage:
        encoding = message.content_encoding if message.content_encoding is not None else self.__encoding
        try:
            body = self.__encoder.encode(message.body).encode(encoding)

        except ValueError as err:
            details = "can't encode json message"
            raise SerializerDumpError(details, message) from err

        return replace(
            message.with_body(body),
            content_type=self.__CONTENT_TYPE,
            content_encoding=encoding,
        )

    def load_message(self, message: RawMessage) -> Message[object]:
        if message.content_type != self.__CONTENT_TYPE:
            details = "can't decode non-json message"
            raise SerializerLoadError(details, message)

        try:
            json_str = message.body.decode(
                message.content_encoding if message.content_encoding is not None else self.__encoding
            )

        except UnicodeDecodeError as err:
            details = f"can't decode binary string: {err}"
            raise SerializerLoadError(details, message) from err

        try:
            obj = self.__decoder.decode(json_str)

        except JSONDecodeError as err:
            details = f"can't decode json message: {err}"
            raise SerializerLoadError(details, message) from err

        return message.with_body(obj)


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

        return replace(
            message.with_body(body),
            content_type=self.__CONTENT_TYPE,
            content_encoding=None,
            message_type=message.body.DESCRIPTOR.full_name,
        )

    def load_message(self, message: RawMessage) -> Message[T]:
        if message.content_type != self.__CONTENT_TYPE:
            details = "invalid content type"
            raise SerializerLoadError(details, message)

        if message.message_type != self.__message_type.DESCRIPTOR.full_name:
            details = "invalid message type"
            raise SerializerLoadError(details, message)

        try:
            body = self.__message_type.FromString(message.body)

        except DecodeError as err:
            details = f"can't decode protobuf message: {err}"
            raise SerializerLoadError(details, message) from err

        return message.with_body(body)


def pack_protobuf_any(msg: ProtobufMessage) -> Any:
    result = Any()
    result.Pack(msg)
    return result


def unpack_protobuf_any[T: ProtobufMessage](msg_type: t.Type[T], value: Any) -> tuple[bool, T]:
    msg = msg_type()
    return value.Unpack(msg), msg
