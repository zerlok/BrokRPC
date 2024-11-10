from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

type PublisherResult = bool | None


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


class ProtomqError(Exception):
    pass


class BrokerError(ProtomqError):
    pass


class PublisherError(ProtomqError):
    pass


class ConsumerError(ProtomqError):
    pass


class SerializerError(ProtomqError):
    pass


class SerializerLoadError(SerializerError, ValueError):
    pass


class SerializerDumpError(SerializerError, ValueError):
    pass
