from __future__ import annotations

import typing as t
from dataclasses import asdict, dataclass, replace

if t.TYPE_CHECKING:
    from datetime import datetime, timedelta

    from _typeshed import DataclassInstance
    from yarl import URL


from protomq.retry import RetryOptions


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
class BrokerOptions(RetryOptions):
    driver: str
    url: URL


@dataclass(frozen=True, kw_only=True)
class QOSOptions:
    prefetch_count: int | None = None

    def __post_init__(self) -> None:
        if self.prefetch_count is not None and self.prefetch_count < 0:
            details = "prefetch count must be greater than or equal to 0"
            raise ValueError(details, self)


@dataclass(frozen=True, kw_only=True)
class ExchangeOptions(QOSOptions):
    name: str | None = None
    type: ExchangeType | None = None
    durable: bool | None = None
    auto_delete: bool | None = None
    arguments: t.Mapping[str, object] | None = None


@dataclass(frozen=True, kw_only=True)
class QueueOptions(QOSOptions):
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

    def __post_init__(self) -> None:
        if not self.binding_keys:
            details = "at least one binding key must be set"
            raise ValueError(details, self)


@dataclass(frozen=True, kw_only=True)
class ConsumerOptions(QueueOptions, RetryOptions, DecoratorOptions):
    retry_on_error: bool | type[Exception] | tuple[type[Exception], ...] | None = None


@dataclass(frozen=True, kw_only=True)
class PublisherOptions(ExchangeOptions):
    mandatory: bool | None = None


@dataclass(frozen=True, kw_only=True)
class MessageOptions:
    # routing_key: str | None = None
    exchange: str | None = None
    content_type: str | None = None
    content_encoding: str | None = None
    headers: t.Mapping[str, str] | None = None
    delivery_mode: int | None = None
    priority: int | None = None
    correlation_id: str | None = None
    reply_to: str | None = None
    timeout: timedelta | None = None
    message_id: str | None = None
    timestamp: datetime | None = None
    message_type: str | None = None
    user_id: str | None = None
    app_id: str | None = None


def merge_options[T: DataclassInstance](*options: T | None) -> T | None:
    options_iter = iter(options)

    result: T | None = None

    for result in options_iter:
        if result is not None:
            break

    if result is None:
        return None

    for opt in options_iter:
        if opt is None:
            continue

        result = replace(result, **{k: v for k, v in asdict(opt).items() if v is not None})

    return result
