from __future__ import annotations

import typing as t
from dataclasses import asdict, dataclass, replace
from datetime import timedelta

from protomq.debug import get_verbosity_level
from protomq.options import MessageOptions


@dataclass(frozen=True, kw_only=True)
class Message[T](MessageOptions):
    @classmethod
    def from_options(cls, body: T, options: MessageOptions) -> Message[T]:
        return cls(body=body, **asdict(options))

    body: T
    # routing_key: str
    # exchange: str | None = None
    # content_type: str | None = None
    # content_encoding: str | None = None
    # headers: t.Mapping[str, str] | None = None
    # delivery_mode: int | None = None
    # priority: int | None = None
    # correlation_id: str | None = None
    # reply_to: str | None = None
    # expiration: str | None = None
    # message_id: str | None = None
    # timestamp: datetime | None = None
    # message_type: str | None = None
    # user_id: str | None = None
    # app_id: str | None = None

    def with_body[V](self, body: V) -> Message[V]:
        return replace(t.cast(Message[V], self), body=body)

    if get_verbosity_level() <= 1:

        def __str__(self) -> str:
            return (
                f"<{self.__class__.__name__} at {hex(id(self))}; id={self.message_id}; "
                f"exchange={self.exchange}; rk={self.routing_key}>"
            )

    else:

        def __str__(self) -> str:
            return repr(self)


type RawMessage = Message[bytes]


type PublisherResult = None


@dataclass(frozen=True, kw_only=True)
class ConsumerAck:
    pass


@dataclass(frozen=True, kw_only=True)
class ConsumerReject:
    reason: str | None = None


@dataclass(frozen=True, kw_only=True)
class ConsumerRetry:
    reason: str | None = None
    delay: timedelta | None


type ConsumerResult = ConsumerAck | ConsumerReject | ConsumerRetry | bool | None
