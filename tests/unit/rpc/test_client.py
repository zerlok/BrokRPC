import pytest

from brokrpc.abc import Serializer
from brokrpc.broker import Broker, BrokerIsNotConnectedError
from brokrpc.message import BinaryMessage, Message
from brokrpc.rpc.abc import CallerSerializer
from brokrpc.rpc.client import Client
from tests.conftest import BROKER_IS_CONNECTED, BROKER_IS_NOT_CONNECTED


@BROKER_IS_CONNECTED
async def test_publisher_ok[U](
    client: Client,
    stub_routing_key: str,
    mock_serializer: Serializer[Message[U], BinaryMessage],
) -> None:
    async with client.publisher(routing_key=stub_routing_key, serializer=mock_serializer):
        pass


@BROKER_IS_NOT_CONNECTED
async def test_publisher_error[U](
    client: Client,
    stub_routing_key: str,
    mock_serializer: Serializer[Message[U], BinaryMessage],
) -> None:
    with pytest.raises(BrokerIsNotConnectedError):
        async with client.publisher(routing_key=stub_routing_key, serializer=mock_serializer):
            pass


@BROKER_IS_CONNECTED
async def test_unary_unary_caller_ok[U, V](
    client: Client,
    stub_routing_key: str,
    mock_rpc_serializer: CallerSerializer[U, V],
) -> None:
    async with client.unary_unary_caller(routing_key=stub_routing_key, serializer=mock_rpc_serializer):
        pass


@BROKER_IS_NOT_CONNECTED
async def test_unary_unary_caller_error[U, V](
    client: Client,
    stub_routing_key: str,
    mock_rpc_serializer: CallerSerializer[U, V],
) -> None:
    with pytest.raises(BrokerIsNotConnectedError):
        async with client.unary_unary_caller(routing_key=stub_routing_key, serializer=mock_rpc_serializer):
            pass


@pytest.fixture
def client(stub_broker: Broker) -> Client:
    return Client(stub_broker)
