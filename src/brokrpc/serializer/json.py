import typing as t
from json import JSONDecodeError, JSONDecoder, JSONEncoder

from brokrpc.abc import Serializer
from brokrpc.message import BinaryMessage, Message, PackedMessage, UnpackedMessage
from brokrpc.model import SerializerDumpError, SerializerLoadError
from brokrpc.rpc.abc import RPCSerializer
from brokrpc.rpc.model import BinaryRequest, Request, Response


class JSONSerializer(Serializer[Message[object], Message[bytes]], RPCSerializer[object, object]):
    __CONTENT_TYPE: t.Final[str] = "application/json"

    def __init__(
        self,
        *,
        encoder: JSONEncoder | None = None,
        decoder: JSONDecoder | None = None,
        encoding: str | None = None,
        default: t.Callable[[object], object] | None = None,
        strict: bool = False,
    ) -> None:
        self.__encoder = (
            encoder
            if encoder is not None
            else JSONEncoder(
                indent=None,
                separators=(",", ":"),
                default=default,
            )
        )
        self.__decoder = decoder if decoder is not None else JSONDecoder()
        self.__encoding = encoding if encoding is not None else "utf-8"
        self.__strict = strict

    def dump_message(self, message: Message[object]) -> PackedMessage[bytes]:
        encoding = message.content_encoding if message.content_encoding is not None else self.__encoding
        try:
            body = self.__encoder.encode(message.body).encode(encoding)

        except ValueError as err:
            details = "can't encode json message"
            raise SerializerDumpError(details, message) from err

        return PackedMessage(
            original=message,
            body=body,
            content_type=self.__CONTENT_TYPE,
            content_encoding=encoding,
        )

    def load_message(self, message: BinaryMessage) -> UnpackedMessage[object]:
        obj = self.__load(message)
        return UnpackedMessage(message, obj)

    def dump_unary_request(self, request: Request[object]) -> BinaryRequest:
        return self.dump_message(request)

    def load_unary_request(self, request: BinaryRequest) -> Request[object]:
        return self.load_message(request)

    def dump_unary_response(self, response: Response[object]) -> BinaryMessage:
        return self.dump_message(response)

    def load_unary_response(self, response: BinaryMessage) -> Response[object]:
        return self.load_message(response)

    def __load(self, message: BinaryMessage) -> object:
        if message.content_type != self.__CONTENT_TYPE and (self.__strict or message.content_type is not None):
            details = f"invalid content type: {message.content_type}"
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

        return obj
