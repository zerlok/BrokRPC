import typing as t

import pytest

from brokrpc.broker import Broker
from brokrpc.options import BrokerOptions
from brokrpc.rpc.abc import RPCSerializer
from brokrpc.rpc.client import Client
from brokrpc.rpc.server import Server
from brokrpc.serializer.json import JSONSerializer
from brokrpc.serializer.protobuf import RPCProtobufSerializer
from tests.stub.proto.greeting_pb2 import GreetingRequest, GreetingResponse


@pytest.fixture
async def broker(broker_options: BrokerOptions) -> t.AsyncIterator[Broker]:
    async with Broker(broker_options) as broker:
        yield broker


@pytest.fixture
def rpc_server(broker: Broker) -> Server:
    return Server(broker)


@pytest.fixture
async def running_rpc_server(rpc_server: Server) -> t.AsyncIterator[Server]:
    async with rpc_server.run():
        yield rpc_server


@pytest.fixture
def rpc_client(broker: Broker) -> Client:
    return Client(broker)


@pytest.fixture
def json_serializer() -> JSONSerializer:
    return JSONSerializer()


@pytest.fixture
def rpc_greeting_serializer() -> RPCSerializer[GreetingRequest, GreetingResponse]:
    return RPCProtobufSerializer(GreetingRequest, GreetingResponse)
