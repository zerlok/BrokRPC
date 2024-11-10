from __future__ import annotations

from protomq.message import Message, PackedMessage, UnpackedMessage
from protomq.model import ProtomqError

type Request[T] = Message[T]
type BinaryRequest = Message[bytes]
type PackedRequest[T] = PackedMessage[T]
type UnpackedRequest[T] = UnpackedMessage[T]

type Response[T] = Message[T]
type BinaryResponse = Message[bytes]
type PackedResponse[T] = PackedMessage[T]
type UnpackedResponse[T] = UnpackedMessage[T]


class ServerError(ProtomqError):
    pass


class HandlerError(ServerError):
    pass


class ClientError(ProtomqError):
    pass


class CallerError(ClientError):
    pass
