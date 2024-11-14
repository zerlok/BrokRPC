from __future__ import annotations

import typing as t
from dataclasses import dataclass

if t.TYPE_CHECKING:
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


class BrokRPCError(Exception):
    pass


class BrokerError(BrokRPCError):
    pass


class PublisherError(BrokRPCError):
    pass


class ConsumerError(BrokRPCError):
    pass


class SerializerError(BrokRPCError):
    pass


class SerializerLoadError(SerializerError, ValueError):
    pass


class SerializerDumpError(SerializerError, ValueError):
    pass
