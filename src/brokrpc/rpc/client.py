import typing as t
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta

from brokrpc.abc import Publisher, Serializer
from brokrpc.broker import Broker
from brokrpc.message import BinaryMessage, Message, create_message
from brokrpc.model import PublisherResult
from brokrpc.options import BindingOptions, ExchangeOptions, PublisherOptions, QueueOptions, merge_options
from brokrpc.rpc.abc import Caller, CallerSerializer
from brokrpc.rpc.caller import RequestCaller
from brokrpc.rpc.handler import HandlerResponseConsumer
from brokrpc.rpc.model import Response
from brokrpc.rpc.storage import WaiterStorage


class Client:
    def __init__(self, broker: Broker) -> None:
        self.__broker = broker

    def publisher[U](
        self,
        *,
        routing_key: str,
        serializer: Serializer[Message[U], BinaryMessage],
        exchange: ExchangeOptions | None = None,
    ) -> t.AsyncContextManager[Publisher[U, PublisherResult]]:
        def build_message(body: U) -> Message[U]:
            return create_message(
                body=body,
                routing_key=routing_key,
                exchange=exchange.name if exchange is not None else None,
            )

        return (
            self.__broker.build_publisher()
            .add_serializer(serializer)
            .add_serializer(build_message)
            .build(PublisherOptions.from_exchange(exchange) if exchange is not None else None)
        )

    @asynccontextmanager
    async def unary_unary_caller[U, V](
        self,
        *,
        routing_key: str,
        serializer: CallerSerializer[U, V],
        exchange: ExchangeOptions | None = None,
        queue: QueueOptions | None = None,
        # NOTE: `timeout` parameter passed to server via message, so server can cancel the task if it is expired.
        timeout: timedelta | None = None,  # noqa: ASYNC109
    ) -> t.AsyncIterator[Caller[U, V]]:
        caller_id = uuid.uuid4()
        response_key = f"response.{routing_key}.{caller_id.hex}"
        binding = self.__get_binding_options(exchange, response_key, queue)

        with WaiterStorage[Response[V]].create() as storage:
            async with (
                self.__broker.consumer(
                    HandlerResponseConsumer(serializer.load_unary_response, storage),
                    binding,
                ),
                self.__broker.publisher() as requester,
            ):
                yield RequestCaller(
                    requester=requester,
                    exchange=binding.exchange,
                    routing_key=routing_key,
                    serializer=serializer.dump_unary_request,
                    reply_to=response_key,
                    storage=storage,
                    timeout=timeout,
                )

    def __get_binding_options(
        self,
        exchange: ExchangeOptions | None,
        response_key: str,
        queue: QueueOptions | None,
    ) -> BindingOptions:
        return BindingOptions(
            exchange=exchange,
            binding_keys=(response_key,),
            queue=merge_options(
                queue,
                QueueOptions(
                    name=response_key,
                    auto_delete=True,
                    exclusive=True,
                ),
            ),
        )
