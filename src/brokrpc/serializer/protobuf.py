import typing as t

from google.protobuf.any_pb2 import Any
from google.protobuf.json_format import Error, MessageToDict, MessageToJson
from google.protobuf.message import DecodeError, EncodeError
from google.protobuf.message import Message as ProtobufMessage

from brokrpc.abc import Serializer
from brokrpc.message import BinaryMessage, DecodedMessage, EncodedMessage, Message
from brokrpc.model import SerializerDumpError, SerializerLoadError
from brokrpc.rpc.abc import RPCSerializer
from brokrpc.rpc.model import BinaryRequest, BinaryResponse, Request, Response
from brokrpc.serializer.json import JSONSerializer


class ProtobufSerializer[T: ProtobufMessage](Serializer[Message[T], Message[bytes]]):
    __CONTENT_TYPE: t.Final[str] = "application/protobuf"

    def __init__(self, message_type: type[T]) -> None:
        self.__message_type = message_type

    def encode_message(self, message: Message[T]) -> EncodedMessage[bytes]:
        assert isinstance(message.body, self.__message_type)

        try:
            body = message.body.SerializeToString()

        except EncodeError as err:
            details = f"can't encode protobuf message: {err}"
            raise SerializerDumpError(details, message) from err

        return EncodedMessage(
            original=message,
            body=body,
            content_type=self.__CONTENT_TYPE,
            content_encoding=None,
            message_type=_get_full_name(message.body),
        )

    def decode_message(self, message: BinaryMessage) -> DecodedMessage[T]:
        if message.content_type != self.__CONTENT_TYPE:
            details = f"invalid content type: {message.content_type}"
            raise SerializerLoadError(details, message)

        if message.message_type != _get_full_name(self.__message_type):
            details = f"invalid message type: {message.message_type}"
            raise SerializerLoadError(details, message)

        try:
            body = self.__message_type.FromString(message.body)

        except DecodeError as err:
            details = f"can't decode protobuf message: {err}"
            raise SerializerLoadError(details, message) from err

        return DecodedMessage(
            original=message,
            body=body,
        )


class DictProtobufSerializer[T: ProtobufMessage](Serializer[Message[dict[str, object]], Message[bytes]]):
    def __init__(self, message_type: type[T]) -> None:
        self.__message_type = message_type
        self.__proto = ProtobufSerializer(message_type)

    def encode_message(self, message: Message[dict[str, object]]) -> Message[bytes]:
        try:
            payload = self.__message_type(**message.body)

        except (TypeError, ValueError) as err:
            details = "can't construct protobuf message"
            raise SerializerDumpError(details, message.body) from err

        return self.__proto.encode_message(EncodedMessage(body=payload, original=message))

    def decode_message(self, message: Message[bytes]) -> Message[dict[str, object]]:
        protobuf_message = self.__proto.decode_message(message)

        try:
            dict_payload: dict[str, object] = MessageToDict(protobuf_message.body, preserving_proto_field_name=True)

        except Error as err:
            details = "can't covert protobuf message to dict"
            raise SerializerLoadError(details, protobuf_message.body) from err

        return DecodedMessage(
            original=protobuf_message,
            body=dict_payload,
        )


class JSONProtobufSerializer[T: ProtobufMessage](Serializer[Message[bytes], Message[bytes]]):
    def __init__(self, message_type: type[T], encoding: str | None = None) -> None:
        self.__message_type = message_type
        self.__json = JSONSerializer(encoding=encoding)
        self.__proto = ProtobufSerializer(message_type)

    def encode_message(self, message: Message[bytes]) -> Message[bytes]:
        try:
            json_message = self.__json.decode_message(
                EncodedMessage(
                    body=message.body,
                    content_type="application/json",
                    content_encoding=None,
                    message_type=None,
                    original=message,
                )
            )

        except SerializerLoadError as err:
            details = "can't load json message"
            raise SerializerDumpError(details, message) from err

        if not self.__check_json_message(json_message.body):
            details = "can't dump non object json to protobuf"
            raise SerializerDumpError(details, json_message)

        try:
            payload = self.__message_type(**json_message.body)

        except (TypeError, ValueError) as err:
            details = "can't construct protobuf message"
            raise SerializerDumpError(details, message.body) from err

        return self.__proto.encode_message(EncodedMessage(body=payload, original=message))

    def decode_message(self, message: Message[bytes]) -> Message[bytes]:
        protobuf_message = self.__proto.decode_message(message)

        try:
            json_payload = MessageToJson(protobuf_message.body, preserving_proto_field_name=True, indent=None)

        except Error as err:
            details = "can't covert protobuf message to json"
            raise SerializerLoadError(details, protobuf_message.body) from err

        return DecodedMessage(
            original=protobuf_message,
            body=json_payload.encode(),
        )

    def __check_json_message(self, obj: object) -> t.TypeGuard[dict[str, object]]:
        return isinstance(obj, dict) and all(isinstance(key, str) for key in obj)


class RPCProtobufSerializer[U: ProtobufMessage, V: ProtobufMessage](RPCSerializer[U, V]):
    def __init__(self, request_type: type[U], response_type: type[V]) -> None:
        self.__request = ProtobufSerializer(request_type)
        self.__response = ProtobufSerializer(response_type)

    def dump_unary_request(self, request: Request[U]) -> BinaryRequest:
        return self.__request.encode_message(request)

    def load_unary_request(self, request: BinaryRequest) -> Request[U]:
        return self.__request.decode_message(request)

    def dump_unary_response(self, response: Response[V]) -> BinaryResponse:
        return self.__response.encode_message(response)

    def load_unary_response(self, response: BinaryResponse) -> Response[V]:
        return self.__response.decode_message(response)


def pack_any(msg: ProtobufMessage) -> Any:
    result = Any()
    result.Pack(msg)
    return result


def unpack_any[T: ProtobufMessage](msg_type: type[T], value: Any) -> tuple[bool, T]:
    msg = msg_type()
    return value.Unpack(msg), msg


def _get_full_name(message: ProtobufMessage | type[ProtobufMessage]) -> str:
    # NOTE: google protobuf message descriptor fields are typed with Any
    return message.DESCRIPTOR.full_name  # type: ignore[misc,no-any-return]
