from __future__ import annotations

__all__ = [
    "BinaryMessage",
    "DecodedMessage",
    "EncodedMessage",
    "Message",
    "SomeMessage",
    "create_message",
]


import abc
import typing as t

if t.TYPE_CHECKING:
    from datetime import datetime, timedelta

from brokrpc.debug import is_debug_enabled
from brokrpc.stringify import to_str_obj


class Message[T](metaclass=abc.ABCMeta):
    """
    Base abstract class for all message structures.

    It is recommended to use this class in user code as interface for all messages.
    """

    if not is_debug_enabled():

        def __str__(self) -> str:
            return to_str_obj(self, type_vars=[type(self.body)], id=self.message_id)

    else:

        def __str__(self) -> str:
            return repr(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented

        return (
            self.body == other.body
            and self.routing_key == other.routing_key
            and self.exchange == other.exchange
            and self.content_type == other.content_type
            and self.content_encoding == other.content_encoding
            and self.headers == other.headers
            and self.delivery_mode == other.delivery_mode
            and self.priority == other.priority
            and self.correlation_id == other.correlation_id
            and self.reply_to == other.reply_to
            and self.timeout == other.timeout
            and self.message_id == other.message_id
            and self.timestamp == other.timestamp
            and self.message_type == other.message_type
            and self.user_id == other.user_id
            and self.app_id == other.app_id
        )

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented

        return not (self == other)

    @property
    @abc.abstractmethod
    def body(self) -> T:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def routing_key(self) -> str:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def exchange(self) -> str | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def content_type(self) -> str | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def content_encoding(self) -> str | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def headers(self) -> t.Mapping[str, str] | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def delivery_mode(self) -> int | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def priority(self) -> int | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def correlation_id(self) -> str | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def reply_to(self) -> str | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def timeout(self) -> timedelta | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def message_id(self) -> str | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def timestamp(self) -> datetime | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def message_type(self) -> str | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def user_id(self) -> str | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def app_id(self) -> str | None:
        raise NotImplementedError


type BinaryMessage = Message[bytes]


# NOTE: message constructor has a lot of options to set up a structure (dataclass)
def create_message[T](  # noqa: PLR0913
    *,
    body: T,
    routing_key: str,
    exchange: str | None = None,
    content_type: str | None = None,
    content_encoding: str | None = None,
    headers: t.Mapping[str, str] | None = None,
    delivery_mode: int | None = None,
    priority: int | None = None,
    correlation_id: str | None = None,
    reply_to: str | None = None,
    timeout: timedelta | None = None,
    message_id: str | None = None,
    timestamp: datetime | None = None,
    message_type: str | None = None,
    user_id: str | None = None,
    app_id: str | None = None,
) -> Message[T]:
    """
    A factory function to create a message.

    Applications should use this function to create messages to send them via brokrpc. The `body` is an application
    level data. Use publisher encoders for data to be encoded before actual sending.
    """

    return SomeMessage(
        body=body,
        routing_key=routing_key,
        exchange=exchange,
        content_type=content_type,
        content_encoding=content_encoding,
        headers=headers,
        delivery_mode=delivery_mode,
        priority=priority,
        correlation_id=correlation_id,
        reply_to=reply_to,
        timeout=timeout,
        message_id=message_id,
        timestamp=timestamp,
        message_type=message_type,
        user_id=user_id,
        app_id=app_id,
    )


class SomeMessage[T](Message[T]):
    """
    Base implementation for message with some data (either application level or broker level).

    Applications should not use this class directly. Use `create_message` factory function instead.
    """

    __slots__ = (
        "__app_id",
        "__body",
        "__content_encoding",
        "__content_type",
        "__correlation_id",
        "__delivery_mode",
        "__exchange",
        "__headers",
        "__message_id",
        "__message_type",
        "__priority",
        "__reply_to",
        "__routing_key",
        "__timeout",
        "__timestamp",
        "__user_id",
    )

    # NOTE: message constructor has a lot of options to set up a structure (dataclass)
    def __init__(  # noqa: PLR0913
        self,
        *,
        body: T,
        routing_key: str,
        exchange: str | None = None,
        content_type: str | None = None,
        content_encoding: str | None = None,
        headers: t.Mapping[str, str] | None = None,
        delivery_mode: int | None = None,
        priority: int | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        timeout: timedelta | None = None,
        message_id: str | None = None,
        timestamp: datetime | None = None,
        message_type: str | None = None,
        user_id: str | None = None,
        app_id: str | None = None,
    ) -> None:
        self.__body = body
        self.__routing_key = routing_key
        self.__exchange = exchange
        self.__content_type = content_type
        self.__content_encoding = content_encoding
        self.__headers = headers
        self.__delivery_mode = delivery_mode
        self.__priority = priority
        self.__correlation_id = correlation_id
        self.__reply_to = reply_to
        self.__timeout = timeout
        self.__message_id = message_id
        self.__timestamp = timestamp
        self.__message_type = message_type
        self.__user_id = user_id
        self.__app_id = app_id

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"body={self.__body!r}, "
            f"routing_key={self.__routing_key!r}, "
            f"exchange={self.__exchange!r}, "
            f"content_type={self.__content_type!r}, "
            f"content_encoding={self.__content_encoding!r}, "
            f"headers={self.__headers!r}, "
            f"delivery_mode={self.__delivery_mode!r}, "
            f"priority={self.__priority!r}, "
            f"correlation_id={self.__correlation_id!r}, "
            f"reply_to={self.__reply_to!r}, "
            f"timeout={self.__timeout!r}, "
            f"message_id={self.__message_id!r}, "
            f"timestamp={self.__timestamp!r}, "
            f"message_type={self.__message_type!r}, "
            f"user_id={self.__user_id!r}, "
            f"app_id={self.__app_id!r}"
            ")"
        )

    @property
    def body(self) -> T:
        return self.__body

    @property
    def routing_key(self) -> str:
        return self.__routing_key

    @property
    def exchange(self) -> str | None:
        return self.__exchange

    @property
    def content_type(self) -> str | None:
        return self.__content_type

    @property
    def content_encoding(self) -> str | None:
        return self.__content_encoding

    @property
    def headers(self) -> t.Mapping[str, str] | None:
        return self.__headers

    @property
    def delivery_mode(self) -> int | None:
        return self.__delivery_mode

    @property
    def priority(self) -> int | None:
        return self.__priority

    @property
    def correlation_id(self) -> str | None:
        return self.__correlation_id

    @property
    def reply_to(self) -> str | None:
        return self.__reply_to

    @property
    def timeout(self) -> timedelta | None:
        return self.__timeout

    @property
    def message_id(self) -> str | None:
        return self.__message_id

    @property
    def timestamp(self) -> datetime | None:
        return self.__timestamp

    @property
    def message_type(self) -> str | None:
        return self.__message_type

    @property
    def user_id(self) -> str | None:
        return self.__user_id

    @property
    def app_id(self) -> str | None:
        return self.__app_id


@t.final
class EncodedMessage[T](Message[T]):
    """
    A message that has an encoded body.

    Applications should not use this class directly. Use publishers with encoders to encode messages before sending.
    """

    __slots__ = (
        "__body",
        "__content_encoding",
        "__content_type",
        "__headers",
        "__message_type",
        "__original",
    )

    # NOTE: message constructor has a lot of options to set up a structure (dataclass)
    def __init__(  # noqa: PLR0913
        self,
        *,
        body: T,
        content_type: str | None = None,
        content_encoding: str | None = None,
        headers: t.Mapping[str, str] | None = None,
        message_type: str | None = None,
        original: Message[object],
    ) -> None:
        self.__body = body
        self.__content_type = content_type if content_type is not None else original.content_type
        self.__content_encoding = content_encoding if content_encoding is not None else original.content_encoding
        self.__headers = headers if headers is not None else original.headers
        self.__message_type = message_type if message_type is not None else original.message_type
        self.__original = original

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"body={self.__body!r}, "
            f"content_type={self.__content_type!r}, "
            f"content_encoding={self.__content_encoding!r}, "
            f"headers={self.__headers!r}, "
            f"message_type={self.__message_type!r}, "
            f"original={self.__original!r}"
            ")"
        )

    @property
    def body(self) -> T:
        return self.__body

    @property
    def routing_key(self) -> str:
        return self.__original.routing_key

    @property
    def exchange(self) -> str | None:
        return self.__original.exchange

    @property
    def content_type(self) -> str | None:
        return self.__content_type

    @property
    def content_encoding(self) -> str | None:
        return self.__content_encoding

    @property
    def headers(self) -> t.Mapping[str, str] | None:
        return self.__headers

    @property
    def delivery_mode(self) -> int | None:
        return self.__original.delivery_mode

    @property
    def priority(self) -> int | None:
        return self.__original.priority

    @property
    def correlation_id(self) -> str | None:
        return self.__original.correlation_id

    @property
    def reply_to(self) -> str | None:
        return self.__original.reply_to

    @property
    def timeout(self) -> timedelta | None:
        return self.__original.timeout

    @property
    def message_id(self) -> str | None:
        return self.__original.message_id

    @property
    def timestamp(self) -> datetime | None:
        return self.__original.timestamp

    @property
    def message_type(self) -> str | None:
        return self.__message_type

    @property
    def user_id(self) -> str | None:
        return self.__original.user_id

    @property
    def app_id(self) -> str | None:
        return self.__original.app_id


@t.final
class DecodedMessage[T](Message[T]):
    """
    A message that has a decoded body.

    Applications should not use this class directly. Use consumer with decoder to decode messages before handling them
    with application level data in `body`.
    """

    __slots__ = (
        "__body",
        "__original",
    )

    def __init__(self, original: Message[object], body: T) -> None:
        self.__original = original
        self.__body = body

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(original={self.__original!r}, body={self.__body!r})"

    @property
    def original(self) -> Message[object]:
        return self.__original

    @property
    def body(self) -> T:
        return self.__body

    @property
    def routing_key(self) -> str:
        return self.__original.routing_key

    @property
    def exchange(self) -> str | None:
        return self.__original.exchange

    @property
    def content_type(self) -> str | None:
        return self.__original.content_type

    @property
    def content_encoding(self) -> str | None:
        return self.__original.content_encoding

    @property
    def headers(self) -> t.Mapping[str, str] | None:
        return self.__original.headers

    @property
    def delivery_mode(self) -> int | None:
        return self.__original.delivery_mode

    @property
    def priority(self) -> int | None:
        return self.__original.priority

    @property
    def correlation_id(self) -> str | None:
        return self.__original.correlation_id

    @property
    def reply_to(self) -> str | None:
        return self.__original.reply_to

    @property
    def timeout(self) -> timedelta | None:
        return self.__original.timeout

    @property
    def message_id(self) -> str | None:
        return self.__original.message_id

    @property
    def timestamp(self) -> datetime | None:
        return self.__original.timestamp

    @property
    def message_type(self) -> str | None:
        return self.__original.message_type

    @property
    def user_id(self) -> str | None:
        return self.__original.user_id

    @property
    def app_id(self) -> str | None:
        return self.__original.app_id
