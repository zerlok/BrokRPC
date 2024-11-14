import typing as t

from brokrpc.abc import Consumer, Publisher, Serializer
from brokrpc.message import Message
from brokrpc.model import ConsumerAck, ConsumerReject, ConsumerResult
from brokrpc.options import MessageOptions
from brokrpc.rpc.abc import RPCSerializer, UnaryUnaryFunc
from brokrpc.rpc.storage import WaiterStorage
from brokrpc.spec.v1.call_pb2 import UnaryRequest, UnaryResponse


class UnaryUnaryConsumer[U, V](Consumer[UnaryRequest, ConsumerResult]):
    def __init__(
        self,
        inner: UnaryUnaryFunc[U, V],
        serializer: RPCSerializer[U, V],
        responder: Publisher[UnaryResponse, None],
    ) -> None:
        self.__inner = inner
        self.__serializer = serializer
        self.__responder = responder

    async def consume(self, request: UnaryRequest) -> ConsumerResult:
        if request.reply_to is None or request.correlation_id is None:
            return ConsumerReject(reason="reply_to and correlation_id are required")

        request_payload = self.__serializer.load_unary_request(request)
        response_payload: V | Exception
        try:
            response_payload = await self.__inner(request_payload)

        # NOTE: Caught exception will be serialized to response and client will receive it.
        except Exception as err:  # noqa: BLE001
            response_payload = err

        response = self.__serializer.dump_response(
            request=request,
            response=response_payload,
            options=MessageOptions(
                exchange=request.exchange,
                routing_key=request.reply_to,
                correlation_id=request.correlation_id,
            ),
        )
        await self.__responder.publish(response)

        return ConsumerAck()


# class UnaryStreamConsumer[U, V](Consumer):
#     def __init__(
#         self,
#         inner: UnaryStreamFunc[U, V],
#         serializer: Serializer[U, V],
#         replier: Publisher,
#     ) -> None:
#         self.__inner = inner
#         self.__serializer = serializer
#         self.__responder = replier
#
#     async def consume(self, request: Message) -> ConsumerResult:
#         if request.reply_to is None or request.correlation_id is None:
#             return ConsumerReject(reason="reply_to and correlation_id are required")
#
#         request_payload = self.__serializer.load_request(request)
#
#         try:
#             async for response_payload in self.__inner(request_payload):
#                 response = self.__serializer.dump_response(
#                     request=request,
#                     response=response_payload,
#                     options=MessageOptions(
#                         exchange=request.exchange,
#                         routing_key=request.reply_to,
#                         correlation_id=request.correlation_id,
#                     ),
#                 )
#                 await self.__responder.publish(response)
#
#         # NOTE: Caught exception will be serialized to response and client will receive it.
#         except Exception as err:
#             await self.__responder.publish(
#                 self.__serializer.dump_response(
#                     request=request,
#                     response=err,
#                     options=MessageOptions(
#                         exchange=request.exchange,
#                         routing_key=request.reply_to,
#                         correlation_id=request.correlation_id,
#                     ),
#                 )
#             )
#
#         return ConsumerAck()


class UnaryResponseConsumer[T](Consumer[Message, ConsumerResult]):
    def __init__(
        self,
        serializer: Serializer[t.Any, T],
        waiters: WaiterStorage[T],
    ) -> None:
        self.__serializer = serializer
        self.__waiters = waiters

    async def consume(self, message: Message) -> ConsumerResult:
        payload = self.__serializer.load_message(message)
        if message.correlation_id is None:
            return ConsumerReject()

        if isinstance(payload, Exception):
            self.__waiters.set_exception(message.correlation_id, payload)

        else:
            self.__waiters.set_result(message.correlation_id, payload)

        return ConsumerAck()
