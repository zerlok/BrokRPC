import typing as t
from contextlib import nullcontext
from datetime import timedelta
from unittest.mock import create_autospec

import pytest

# FIXME: find a way to import Parser
# NOTE: fixes strange error `Module "_pytest.config" does not explicitly export attribute "Parser"`.
from _pytest.config import Config, Parser  # type: ignore[attr-defined]
from _pytest.fixtures import SubRequest
from yarl import URL

from brokrpc.abc import Consumer, Serializer
from brokrpc.broker import Broker
from brokrpc.message import BinaryMessage
from brokrpc.model import ConsumerResult
from brokrpc.options import BindingOptions, BrokerOptions
from brokrpc.retry import ConstantDelay, DelayRetryStrategy, ExponentialDelay, MultiplierDelay
from brokrpc.rpc.abc import RPCSerializer, UnaryUnaryHandler
from tests.stub.driver import StubBrokerDriver, StubConsumer

BROKER_IS_CONNECTED: t.Final = pytest.mark.parametrize(
    "broker_connected", [pytest.param(True, id="broker is connected")]
)
BROKER_IS_NOT_CONNECTED: t.Final = pytest.mark.parametrize(
    "broker_connected", [pytest.param(False, id="broker is not connected")]
)
WARNINGS_AS_ERRORS: t.Final = pytest.mark.WARNINGS_AS_ERRORS("error")


@pytest.hookimpl(trylast=True)
def pytest_addoption(parser: Parser) -> None:
    def parse_seconds(value: str) -> timedelta:
        return timedelta(seconds=float(value))

    parser.addoption(
        "--broker-url",
        type=URL,
        default=URL("amqp://guest:guest@localhost:5672/"),
    )
    parser.addoption(
        "--broker-driver",
        type=str,
        choices=["aiormq"],
        default="aiormq",
    )
    parser.addoption(
        "--broker-retry-delay",
        type=parse_seconds,
        default=None,
    )
    parser.addoption(
        "--broker-retry-delay-mode",
        type=str,
        choices=["constant", "multiplier", "exponential"],
        default=None,
    )
    parser.addoption(
        "--broker-retry-max-delay",
        type=parse_seconds,
        default=None,
    )
    parser.addoption(
        "--broker-retries-timeout",
        type=parse_seconds,
        default=None,
    )
    parser.addoption(
        "--broker-retries-limit",
        type=int,
        default=None,
    )


def parse_broker_options(config: Config) -> BrokerOptions:
    return BrokerOptions(
        url=config.getoption("broker_url"),
        driver=config.getoption("broker_driver"),
        retry_delay=parse_retry_delay_strategy(config),
        retries_timeout=config.getoption("broker_retries_timeout"),
        retries_limit=config.getoption("broker_retries_limit"),
    )


def parse_retry_delay_strategy(config: Config) -> DelayRetryStrategy | None:
    match mode := config.getoption("broker_retry_delay_mode"):
        case "constant":
            return ConstantDelay(
                delay=config.getoption("broker_retry_delay"),
            )

        case "multiplier":
            return MultiplierDelay(
                delay=config.getoption("broker_retry_delay"),
                max_delay=config.getoption("broker_retry_max_delay"),
            )

        case "exponential":
            return ExponentialDelay(
                delay=config.getoption("broker_retry_delay"),
                max_delay=config.getoption("broker_retry_max_delay"),
            )

        case None:
            return None

        case _:
            details = "unknown mode"
            raise ValueError(details, mode)


@pytest.fixture
def stub_broker_driver() -> StubBrokerDriver:
    return StubBrokerDriver()


@pytest.fixture
async def stub_broker(*, stub_broker_driver: StubBrokerDriver, broker_connected: bool) -> t.AsyncIterator[Broker]:
    broker = Broker(nullcontext(stub_broker_driver))
    try:
        if broker_connected:
            await broker.connect()

        assert broker.is_connected is broker_connected

        yield broker

    finally:
        await broker.disconnect()


@pytest.fixture
def broker_connected() -> bool:
    return False


@pytest.fixture
def stub_consumer() -> StubConsumer:
    return StubConsumer()


@pytest.fixture
def mock_consumer() -> Consumer[object, ConsumerResult]:
    return create_autospec(Consumer)


@pytest.fixture
def mock_unary_unary_handler() -> UnaryUnaryHandler[object, object]:
    return create_autospec(UnaryUnaryHandler)


@pytest.fixture
def stub_routing_key(request: SubRequest) -> str:
    return request.node.name


@pytest.fixture
def stub_binding_options(stub_routing_key: str) -> BindingOptions:
    return BindingOptions(binding_keys=(stub_routing_key,))


@pytest.fixture
def mock_serializer() -> Serializer[object, BinaryMessage]:
    return create_autospec(Serializer)


@pytest.fixture
def mock_rpc_serializer() -> RPCSerializer[object, object]:
    return create_autospec(RPCSerializer)
