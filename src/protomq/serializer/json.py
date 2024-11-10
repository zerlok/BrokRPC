import typing as t
from dataclasses import replace
from json import JSONDecodeError, JSONDecoder, JSONEncoder

from protomq.abc import Serializer
from protomq.exception import SerializerDumpError, SerializerLoadError
from protomq.model import Message, RawMessage
from protomq.rpc.abc import RPCSerializer
from protomq.rpc.model import Request, Response


class JSONSerializer(Serializer[Message[object], RawMessage], RPCSerializer[object, object]):
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
            message.repack(body),
            content_type=self.__CONTENT_TYPE,
            content_encoding=encoding,
        )

    def load_message(self, message: RawMessage) -> Message[object]:
        obj = self.__load(message)
        return message.repack(obj)

    def dump_unary_request(self, request: Request[object]) -> Request[bytes]:
        return self.dump_message(request)

    def load_unary_request(self, request: Request[bytes]) -> object:
        return self.__load(request)

    def dump_unary_response(self, response: Response[object, object]) -> RawMessage:
        return self.dump_message(response)

    def load_unary_response(self, response: RawMessage) -> object:
        return self.__load(response)

    def __load(self, message: RawMessage) -> object:
        if message.content_type != self.__CONTENT_TYPE:
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
