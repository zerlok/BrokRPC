import typing as t
from contextlib import nullcontext
from datetime import timedelta

import pytest

# FIXME: find a way to import Parser
# NOTE: fixes strange error `Module "_pytest.config" does not explicitly export attribute "Parser"`.
from _pytest.config import Config, Parser  # type: ignore[attr-defined]
from yarl import URL

from brokrpc.abc import BrokerDriver
from brokrpc.broker import Broker
from brokrpc.options import BrokerOptions
from tests.stub.driver import StubBrokerDriver, StubConsumer


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
        retry_delay=config.getoption("broker_retry_delay"),
        retry_delay_mode=config.getoption("broker_retry_delay_mode"),
        retry_max_delay=config.getoption("broker_retry_max_delay"),
        retries_timeout=config.getoption("broker_retries_timeout"),
        retries_limit=config.getoption("broker_retries_limit"),
    )


@pytest.fixture
def broker_driver() -> StubBrokerDriver:
    return StubBrokerDriver()


@pytest.fixture
def broker(broker_driver: BrokerDriver) -> Broker:
    return Broker(nullcontext(broker_driver))


@pytest.fixture
async def connected_broker(broker: Broker) -> t.AsyncIterator[Broker]:
    async with broker:
        yield broker


@pytest.fixture
def stub_consumer() -> StubConsumer:
    return StubConsumer()
