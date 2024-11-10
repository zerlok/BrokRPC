import typing as t
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta

from protomq.broker import Broker
from protomq.options import BindingOptions, ExchangeOptions, QueueOptions
from protomq.rpc.abc import Caller, CallerSerializer
from protomq.rpc.caller import RequestCaller
from protomq.rpc.handler import HandlerResponseConsumer
from protomq.rpc.storage import WaiterStorage


class Client:
    def __init__(self, broker: Broker) -> None:
        self.__broker = broker

    @asynccontextmanager
    async def unary_unary_caller[U, V](
        self,
        routing_key: str,
        serializer: CallerSerializer[U, V],
        exchange: ExchangeOptions | None = None,
        queue: QueueOptions | None = None,
        timeout: timedelta | None = None,
    ) -> t.AsyncIterator[Caller[U, V]]:
        caller_id = uuid.uuid4()
        response_key = f"response.{routing_key}.{caller_id.hex}"

        with WaiterStorage[V].create() as storage:
            async with (
                self.__broker.consumer(
                    HandlerResponseConsumer(serializer.load_unary_response, storage),
                    self.__get_binding_options(exchange, response_key, queue),
                ),
                self.__broker.publisher() as requester,
            ):
                yield RequestCaller(
                    requester=requester,
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
            queue=replace(
                queue if queue is not None else QueueOptions(),
                name=response_key,
                auto_delete=True,
                exclusive=True,
            ),
        )
