from __future__ import annotations

from brokrpc.message import Message, PackedMessage, UnpackedMessage
from brokrpc.model import BrokRPCError

type Request[T] = Message[T]
type BinaryRequest = Message[bytes]
type PackedRequest[T] = PackedMessage[T]
type UnpackedRequest[T] = UnpackedMessage[T]

type Response[T] = Message[T]
type BinaryResponse = Message[bytes]
type PackedResponse[T] = PackedMessage[T]
type UnpackedResponse[T] = UnpackedMessage[T]


class ServerError(BrokRPCError):
    pass


class HandlerError(ServerError):
    pass


class ClientError(BrokRPCError):
    pass


class CallerError(ClientError):
    pass
