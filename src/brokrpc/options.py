from __future__ import annotations

import typing as t
from dataclasses import asdict, dataclass, replace

if t.TYPE_CHECKING:
    from _typeshed import DataclassInstance
    from yarl import URL


from brokrpc.retry import DelayRetryOptions


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
class BrokerOptions(DelayRetryOptions):
    driver: str
    url: URL


@dataclass(frozen=True, kw_only=True)
class BrokerEntityOptions:
    mode: t.Literal["soft", "strict"] | None = None
    create_if_not_exist: bool | None = None
    name: str | None = None


@dataclass(frozen=True, kw_only=True)
class QOSOptions:
    prefetch_count: int | None = None

    def __post_init__(self) -> None:
        if self.prefetch_count is not None and self.prefetch_count < 0:
            details = "prefetch count must be greater than or equal to 0"
            raise ValueError(details, self)


@dataclass(frozen=True, kw_only=True)
class ExchangeOptions(BrokerEntityOptions, QOSOptions):
    type: ExchangeType | None = None
    durable: bool | None = None
    auto_delete: bool | None = None
    arguments: t.Mapping[str, object] | None = None


@dataclass(frozen=True, kw_only=True)
class QueueOptions(BrokerEntityOptions, QOSOptions):
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
        if isinstance(self.binding_keys, str):
            details = (
                "a string (a sequence of chars) can't be used as a list of binding keys, it's a possible typo in "
                "the code (missed comma in tuple), try to provide a sequence of binding keys explicitly"
            )
            # NOTE: `str` is a valid type for `t.Sequence[str]`, but this value for binding_keys is not good.
            raise ValueError(details, list(self.binding_keys))  # noqa: TRY004

        if not self.binding_keys:
            details = "at least one binding key must be set"
            raise ValueError(details, self)


@dataclass(frozen=True, kw_only=True)
class ConsumerOptions(QueueOptions, DecoratorOptions):
    pass


@dataclass(frozen=True, kw_only=True)
class PublisherOptions(ExchangeOptions):
    @classmethod
    def from_exchange(cls, options: ExchangeOptions) -> PublisherOptions:
        return cls(**asdict(options))

    mandatory: bool | None = None


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
