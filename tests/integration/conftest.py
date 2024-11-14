import typing as t
from datetime import timedelta

import pytest
from _pytest.config import Parser
from _pytest.fixtures import SubRequest
from brokrpc.broker import Broker
from brokrpc.options import BrokerOptions
from brokrpc.rpc.client import Client
from brokrpc.rpc.server import Server
from brokrpc.serializer.json import JSONSerializer
from yarl import URL


def pytest_addoption(parser: Parser) -> None:
    def parse_seconds(value: str) -> timedelta:
        return timedelta(seconds=float(value))

    parser.addoption(
        "--broker-url",
        type=URL,
        default=URL("amqp://guest:guest@localhost:5672/"),
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


@pytest.fixture(
    params=[
        pytest.param("aiormq"),
    ]
)
def rabbitmq_options(request: SubRequest) -> BrokerOptions:
    return BrokerOptions(
        url=request.config.getoption("broker_url"),
        driver=request.param,
        retry_delay=request.config.getoption("broker_retry_delay"),
        retry_delay_mode=request.config.getoption("broker_retry_delay_mode"),
        retry_max_delay=request.config.getoption("broker_retry_max_delay"),
        retries_timeout=request.config.getoption("broker_retries_timeout"),
        retries_limit=request.config.getoption("broker_retries_limit"),
    )


@pytest.fixture()
async def rabbitmq_broker(rabbitmq_options: BrokerOptions) -> t.AsyncIterator[Broker]:
    async with Broker(rabbitmq_options) as broker:
        yield broker


@pytest.fixture()
def rpc_server(rabbitmq_broker: Broker) -> Server:
    return Server(rabbitmq_broker)


@pytest.fixture()
async def running_rpc_server(rpc_server: Server) -> t.AsyncIterator[Server]:
    async with rpc_server.run():
        yield rpc_server


@pytest.fixture()
def rpc_client(rabbitmq_broker: Broker) -> Client:
    return Client(rabbitmq_broker)


@pytest.fixture()
def json_serializer() -> JSONSerializer:
    return JSONSerializer()
