from __future__ import annotations

import typing as t
from dataclasses import dataclass
from datetime import datetime, timedelta

from yarl import URL


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


type ExchangeType = t.Literal["direct", "fanout", "topic", "headers"]


@dataclass(frozen=True, kw_only=True)
class ConnectOptions:
    driver: str
    url: URL
    max_attempts: int = 10
    attempt_delay: timedelta = timedelta(seconds=3.0)

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            details = "max attempts must be greater than 0"
            raise ValueError(details, self.max_attempts)

        if self.attempt_delay.total_seconds() < 0.0:
            details = "attempt delay must be greater than or equal to 0"
            raise ValueError(details, self.attempt_delay)

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"""driver="{self.driver}", """
            f"""url="{self.url.with_password("******")}", """
            f"max_attempts={self.max_attempts}, "
            f"attempt_delay={self.attempt_delay!r}"
            f")"
        )


@dataclass(frozen=True, kw_only=True)
class ChannelOptions:
    prefetch_count: int | None = None


@dataclass(frozen=True, kw_only=True)
class ExchangeOptions(ChannelOptions):
    name: str | None = None
    type: ExchangeType | None = None
    durable: bool | None = None
    auto_delete: bool | None = None
    arguments: t.Mapping[str, object] | None = None


@dataclass(frozen=True, kw_only=True)
class QueueOptions(ChannelOptions):
    name: str | None = None
    durable: bool | None = None
    exclusive: bool | None = None
    auto_delete: bool | None = None
    arguments: t.Mapping[str, object] | None = None


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
    message_id: str | None = None
    timestamp: datetime | None = None
    message_type: str | None = None
    user_id: str | None = None
    app_id: str | None = None
