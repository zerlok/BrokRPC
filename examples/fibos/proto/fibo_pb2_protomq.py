from __future__ import annotations

import abc
import asyncio
import typing
from contextlib import AsyncExitStack, asynccontextmanager

from brokrpc.options import BindingOptions, ExchangeOptions, PublisherOptions
from brokrpc.rpc.caller import UnaryUnaryCaller
from brokrpc.rpc.client import Client
from brokrpc.rpc.server import Server
from brokrpc.serializers.protobuf import ProtobufSerializer

from examples.fibos.proto import fibo_pb2


class FiboConsumer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def count_fibo(self, request: fibo_pb2.FiboRequest) -> fibo_pb2.FiboResponse:
        """Hello world count_fibo!"""
        raise NotImplementedError


def add_fibo_consumer_to_server(consumer: FiboConsumer, server: Server) -> None:
    server.register_unary_unary_consumer(
        func=consumer.count_fibo,
        serializer=ProtobufSerializer(
            request_type=fibo_pb2.FiboRequest,
            response_type=fibo_pb2.FiboResponse,
        ),
        bindings=BindingOptions(
            exchange=ExchangeOptions(name="test"),
            binding_keys=["fibo/CountFibo"],
        ),
    )


class FiboClient:
    @classmethod
    @asynccontextmanager
    async def create(cls, session: Client) -> typing.AsyncIterator[FiboClient]:
        async with AsyncExitStack() as cm_stack:
            handlers = await asyncio.gather(
                cm_stack.enter_async_context(
                    session.register_unary_unary_caller(
                        routing_key="fibo/CountFibo",
                        publisher=PublisherOptions(
                            name="test",
                        ),
                        serializer=ProtobufSerializer(
                            request_type=fibo_pb2.FiboRequest,
                            response_type=fibo_pb2.FiboResponse,
                        ),
                    )
                ),
            )

            yield cls(*handlers)

    def __init__(
        self,
        count_fibo: UnaryUnaryCaller[fibo_pb2.FiboRequest, fibo_pb2.FiboResponse],
    ) -> None:
        self.__count_fibo = count_fibo

    async def count_fibo(self, request: fibo_pb2.FiboRequest) -> fibo_pb2.FiboResponse:
        """Hello world count_fibo!"""
        return await self.__count_fibo.request(request)
