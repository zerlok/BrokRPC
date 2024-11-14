import typing as t
from contextlib import nullcontext

import pytest
from brokrpc.abc import BrokerDriver
from brokrpc.broker import Broker

from tests.stub.driver import StubBrokerDriver, StubConsumer


@pytest.fixture()
def broker_driver() -> StubBrokerDriver:
    return StubBrokerDriver()


@pytest.fixture()
def broker(broker_driver: BrokerDriver) -> Broker:
    return Broker(nullcontext(broker_driver))


@pytest.fixture()
async def connected_broker(broker: Broker) -> t.AsyncIterator[Broker]:
    async with broker:
        yield broker


@pytest.fixture()
def stub_consumer() -> StubConsumer:
    return StubConsumer()
