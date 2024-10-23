import typing as t
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, kw_only=True)
class Message:
    body: bytes
    routing_key: str
    exchange: str | None = None
    content_type: str | None = None
    content_encoding: str | None = None
    headers: t.Mapping[str, str] | None = None
    delivery_mode: int | None = None
    priority: int | None = None
    correlation_id: str | None = None
    reply_to: str | None = None
    expiration: str | None = None
    message_id: str | None = None
    timestamp: datetime | None = None
    message_type: str | None = None
    user_id: str | None = None
    app_id: str | None = None

    def __str__(self) -> str:
        return (
            f"<{self.__class__.__name__} at {hex(id(self))}; id={self.message_id}; body={len(self.body)} bytes; "
            f"exchange={self.exchange}; rk={self.routing_key}>"
        )


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


type ConsumerResult = ConsumerAck | ConsumerReject | ConsumerRetry
