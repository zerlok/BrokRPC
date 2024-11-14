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


class brokrpcError(Exception):
    pass


class BrokerError(brokrpcError):
    pass


class PublisherError(brokrpcError):
    pass


class ConsumerError(brokrpcError):
    pass


class SerializerError(brokrpcError):
    pass


class SerializerLoadError(SerializerError, ValueError):
    pass


class SerializerDumpError(SerializerError, ValueError):
    pass