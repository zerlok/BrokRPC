import typing as t
from dataclasses import replace

import pytest
from _pytest.fixtures import SubRequest

from brokrpc.broker import Broker
from brokrpc.options import BrokerOptions
from brokrpc.rpc.abc import RPCSerializer
from brokrpc.rpc.client import Client
from brokrpc.rpc.server import Server
from brokrpc.serializer.json import JSONSerializer
from brokrpc.serializer.protobuf import RPCProtobufSerializer
from tests.conftest import parse_broker_options
from tests.stub.proto.greeting_pb2 import GreetingRequest, GreetingResponse


@pytest.fixture(
    params=[
        pytest.param("aiormq"),
    ]
)
def rabbitmq_options(request: SubRequest) -> BrokerOptions:
    return replace(
        parse_broker_options(request.config),
        driver=request.param,
    )


@pytest.fixture
async def rabbitmq_broker(rabbitmq_options: BrokerOptions) -> t.AsyncIterator[Broker]:
    async with Broker(rabbitmq_options) as broker:
        yield broker


@pytest.fixture
def rpc_server(rabbitmq_broker: Broker) -> Server:
    return Server(rabbitmq_broker)


@pytest.fixture
async def running_rpc_server(rpc_server: Server) -> t.AsyncIterator[Server]:
    async with rpc_server.run():
        yield rpc_server


@pytest.fixture
def rpc_client(rabbitmq_broker: Broker) -> Client:
    return Client(rabbitmq_broker)


@pytest.fixture
def json_serializer() -> JSONSerializer:
    return JSONSerializer()


@pytest.fixture
def rpc_greeting_serializer() -> RPCSerializer[GreetingRequest, GreetingResponse]:
    return RPCProtobufSerializer(GreetingRequest, GreetingResponse)
