from __future__ import annotations

import typing as t
from dataclasses import asdict, dataclass, replace
from datetime import timedelta

from protomq.debug import get_verbosity_level
from protomq.options import MessageOptions


@dataclass(frozen=True, kw_only=True)
class Message[T](MessageOptions):
    body: T
    routing_key: str
    # options: MessageOptions | None = None

    def repack[V](self, body: V, options: MessageOptions | None = None) -> Message[V]:
        return replace(t.cast(Message[V], self), body=body, **(asdict(options) if options is not None else {}))

    #
    # def unpack[V](self, body: V, options: MessageOptions | None = None) -> Message[V]:
    #     return UnpackedMessage(t.cast(Message[V], self), body=body, **(asdict(options) if options is not None else {}))

    # def with_options(self, options: MessageOptions) -> Message[T]:
    #     return replace(self, **asdict(options))

    if get_verbosity_level() <= 1:

        def __str__(self) -> str:
            return (
                f"<{self.__class__.__name__}[{type(self.body)}] at {hex(id(self))}; id={self.message_id}; "
                f"exchange={self.exchange}; rk={self.routing_key}>"
            )

    else:

        def __str__(self) -> str:
            return repr(self)


type RawMessage = Message[bytes]


# @dataclass(frozen=True, kw_only=True)
# class UnpackedMessage[T](Message[T]):
#     raw: RawMessage
#
#
# @dataclass(frozen=True, kw_only=True)
# class PackedMessage[T](Message[T]):
#     pass


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
