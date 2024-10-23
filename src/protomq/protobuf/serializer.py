import typing as t
from functools import partial

from google.protobuf.message import DecodeError, EncodeError
from google.protobuf.message import Message as ProtobufMessage

from protomq.abc import Serializer
from protomq.exception import CallError, SerializerDumpError, SerializerLoadError
from protomq.message import Message
from protomq.options import MessageOptions


class ProtobufSerializer[U: ProtobufMessage, V: ProtobufMessage](Serializer[U, V]):
    __CONTENT_TYPE: t.Final[str] = "application/protobuf"
    __EXCEPTION_TYPE: t.Final[str] = "exception"

    def __init__(
        self,
        request_type: t.Type[U],
        response_type: t.Type[V],
        strict: bool = True,
    ) -> None:
        self.__load_request = (
            partial(self.load_protobuf_strict, request_type) if strict else partial(self.load_protobuf, request_type)
        )
        self.__load_response = (
            partial(self.load_protobuf_strict, response_type) if strict else partial(self.load_protobuf, response_type)
        )

    def load_request(self, request: Message) -> U:
        return self.__load_request(request)

    def dump_request(self, request: U, options: MessageOptions) -> Message:
        try:
            body = request.SerializeToString()

        except EncodeError as err:
            details = "can't encode request protobuf message"
            raise SerializerDumpError(details, request) from err

        else:
            return Message(
                content_type=options.content_type or self.__CONTENT_TYPE,
                message_type=options.message_type or request.DESCRIPTOR.full_name,
                body=body,
                exchange=options.exchange,
                routing_key=options.routing_key,
                content_encoding=options.content_encoding,
                headers=options.headers,
                delivery_mode=options.delivery_mode,
                priority=options.priority,
                correlation_id=options.correlation_id,
                reply_to=options.reply_to,
                expiration=options.expiration,
                message_id=options.message_id() if options.message_id is not None else None,
                timestamp=options.timestamp() if options.timestamp is not None else None,
                user_id=options.user_id,
                app_id=options.app_id,
            )

    def load_response(self, response: Message) -> V | Exception:
        if response.message_type == self.__EXCEPTION_TYPE:
            return CallError(
                description=response.body.decode().strip(),
                response=response,
            )

        try:
            return self.__load_response(response)

        except SerializerLoadError as err:
            return err

    def dump_response(self, request: Message, response: V | Exception, options: MessageOptions) -> Message:
        if isinstance(response, Exception):
            message_type = self.__EXCEPTION_TYPE
            body = str(response).encode()

        else:
            message_type = options.message_type or response.DESCRIPTOR.full_name
            try:
                body = response.SerializeToString()

            except EncodeError as err:
                details = "can't encode response protobuf message"
                raise SerializerDumpError(details, request, response) from err

        return Message(
            content_type=options.content_type or self.__CONTENT_TYPE,
            message_type=message_type,
            body=body,
            exchange=options.exchange,
            routing_key=options.routing_key,
            content_encoding=options.content_encoding,
            headers=options.headers,
            delivery_mode=options.delivery_mode,
            priority=options.priority,
            correlation_id=options.correlation_id,
            expiration=options.expiration,
            message_id=options.message_id() if options.message_id is not None else None,
            timestamp=options.timestamp() if options.timestamp is not None else None,
            user_id=options.user_id,
            app_id=options.app_id,
        )

    @classmethod
    def load_protobuf_strict[T: ProtobufMessage](cls, message_type: t.Type[T], message: Message) -> T:
        if message.content_type != cls.__CONTENT_TYPE:
            details = "invalid content type"
            raise SerializerLoadError(details, message)

        if message.message_type != message_type.DESCRIPTOR.full_name:
            details = "invalid message type"
            raise SerializerLoadError(details, message)

        return cls.load_protobuf(message_type, message)

    @classmethod
    def load_protobuf[T: ProtobufMessage](cls, message_type: t.Type[T], message: Message) -> T:
        try:
            return message_type.FromString(message.body)

        except DecodeError as err:
            details = "can't decode protobuf body"
            raise SerializerLoadError(details, message) from err
