import typing as t

import pytest

from brokrpc.abc import Consumer, Serializer
from brokrpc.broker import Broker
from brokrpc.message import BinaryMessage
from brokrpc.model import ConsumerResult
from brokrpc.rpc.abc import HandlerSerializer, UnaryUnaryHandler
from brokrpc.rpc.model import Request
from brokrpc.rpc.server import Server, ServerNotInConfigurableStateError, ServerOptions
from tests.conftest import BROKER_IS_CONNECTED, WARNINGS_AS_ERRORS

SERVER_IS_CONFIGURABLE = pytest.mark.parametrize(
    ("broker_connected", "server_running"),
    [
        pytest.param(False, False, id="broker not connected & server is not running"),
        pytest.param(True, False, id="broker is connected, but server is not running yet"),
    ],
)
SERVER_IS_NOT_CONFIGURABLE = pytest.mark.parametrize(
    ("broker_connected", "server_running"),
    [
        pytest.param(True, True, id="broker is connected & server is running"),
    ],
)


@BROKER_IS_CONNECTED
async def test_warning_on_del_of_running_server(stub_broker: Broker) -> None:
    server = Server(stub_broker)

    await server.start()

    with pytest.warns(RuntimeWarning, match="server was not stopped properly"):
        del server


@BROKER_IS_CONNECTED
@WARNINGS_AS_ERRORS
async def test_no_warning_on_del_of_stopped_server(stub_broker: Broker) -> None:
    server = Server(stub_broker)

    await server.start()
    await server.stop()

    del server


@SERVER_IS_CONFIGURABLE
def test_register_consumer_ok[U](
    *,
    server: Server,
    mock_consumer: Consumer[U, ConsumerResult],
    stub_routing_key: str,
    mock_serializer: Serializer[U, BinaryMessage],
) -> None:
    server.register_consumer(mock_consumer, stub_routing_key, mock_serializer)


@SERVER_IS_NOT_CONFIGURABLE
def test_register_consumer_error[U](
    *,
    server: Server,
    mock_consumer: Consumer[U, ConsumerResult],
    stub_routing_key: str,
    mock_serializer: Serializer[U, BinaryMessage],
) -> None:
    with pytest.raises(ServerNotInConfigurableStateError):
        server.register_consumer(mock_consumer, stub_routing_key, mock_serializer)


@SERVER_IS_CONFIGURABLE
def test_register_unary_unary_ok[U, V](
    *,
    server: Server,
    mock_unary_unary_handler: UnaryUnaryHandler[Request[U], V],
    stub_routing_key: str,
    mock_rpc_serializer: HandlerSerializer[U, V],
) -> None:
    server.register_unary_unary_handler(
        func=mock_unary_unary_handler,
        routing_key=stub_routing_key,
        serializer=mock_rpc_serializer,
    )


@SERVER_IS_NOT_CONFIGURABLE
def test_register_unary_unary_error[U, V](
    *,
    server: Server,
    mock_unary_unary_handler: UnaryUnaryHandler[Request[U], V],
    stub_routing_key: str,
    mock_rpc_serializer: HandlerSerializer[U, V],
) -> None:
    with pytest.raises(ServerNotInConfigurableStateError):
        server.register_unary_unary_handler(
            func=mock_unary_unary_handler,
            routing_key=stub_routing_key,
            serializer=mock_rpc_serializer,
        )


@pytest.fixture
async def server(
    *,
    stub_broker: Broker,
    server_options: ServerOptions | None,
    server_running: bool,
) -> t.AsyncIterator[Server]:
    server = Server(stub_broker, server_options)
    try:
        if server_running:
            await server.start()

        yield server

    finally:
        await server.stop()


@pytest.fixture
def server_options() -> ServerOptions | None:
    return None


@pytest.fixture
def server_running() -> bool:
    return False
