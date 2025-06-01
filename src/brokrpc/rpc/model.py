from __future__ import annotations

from brokrpc.message import DecodedMessage, EncodedMessage, Message
from brokrpc.model import BrokRPCError

type Request[T] = Message[T]
type BinaryRequest = Message[bytes]
type PackedRequest[T] = EncodedMessage[T]
type UnpackedRequest[T] = DecodedMessage[T]

type Response[T] = Message[T]
type BinaryResponse = Message[bytes]
type PackedResponse[T] = EncodedMessage[T]
type UnpackedResponse[T] = DecodedMessage[T]


class ServerError(BrokRPCError):
    pass


class HandlerError(ServerError):
    pass


class ClientError(BrokRPCError):
    pass


class CallerError(ClientError):
    pass
