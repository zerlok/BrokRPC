from __future__ import annotations

from dataclasses import dataclass

from protomq.model import Message

type Request[T] = Message[T]


@dataclass(frozen=True, kw_only=True)
class Response[U, V](Message[V]):
    to_request: U
