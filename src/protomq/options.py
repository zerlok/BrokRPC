from __future__ import annotations

import typing as t
from dataclasses import dataclass

if t.TYPE_CHECKING:
    from datetime import datetime, timedelta


class DecoratorOptions:
    @classmethod
    def load(cls, obj: object) -> t.Self:
        return t.cast(t.Self, getattr(obj, cls._get_key(), cls()))

    def __call__[**U, V](self, func: t.Callable[U, V]) -> t.Callable[U, V]:
        setattr(func, self._get_key(), self)
        return func

    @classmethod
    def _get_key(cls) -> str:
        return f"{cls.__module__}.{cls.__name__}"


@t.overload
def flatten[T](*values: T | None) -> T | None: ...


@t.overload
def flatten[T](*values: T | None, default: T) -> T: ...


def flatten[T](*values: T | None, default: T | None = None) -> T | None:
    for value in values:
        if value is not None:
            return value

    return default


type ExchangeType = t.Literal["direct", "fanout", "topic", "headers"]


@dataclass(frozen=True, kw_only=True)
class ChannelOptions:
    prefetch_count: int | None = None


@dataclass(frozen=True, kw_only=True)
class ExchangeOptions(ChannelOptions):
    name: str | None = None
    type: ExchangeType | None = None
    durable: bool | None = None
    auto_delete: bool | None = None


@dataclass(frozen=True, kw_only=True)
class QueueOptions(ChannelOptions):
    name: str | None = None
    durable: bool | None = None
    exclusive: bool | None = None
    auto_delete: bool | None = None


@dataclass(frozen=True, kw_only=True)
class BindingOptions:
    exchange: ExchangeOptions | None = None
    binding_keys: t.Sequence[str]
    queue: QueueOptions | None = None


@dataclass(frozen=True, kw_only=True)
class ConsumerOptions(QueueOptions, DecoratorOptions):
    retry_on_error: bool | type[Exception] | tuple[type[Exception], ...] | None = None
    retry_delay: float | timedelta | None = None
    max_retries: int | None = None


@dataclass(frozen=True, kw_only=True)
class PublisherOptions(ExchangeOptions):
    mandatory: bool | None = None


@dataclass(frozen=True, kw_only=True)
class MessageOptions:
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
    message_id: t.Callable[[], str | None] | None = None
    timestamp: t.Callable[[], datetime | None] | None = None
    message_type: str | None = None
    user_id: str | None = None
    app_id: str | None = None
