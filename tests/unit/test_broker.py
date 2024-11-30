from contextlib import nullcontext

import pytest

from brokrpc.abc import BrokerDriver
from brokrpc.broker import Broker, BrokerIsNotConnectedError
from brokrpc.options import BindingOptions
from tests.conftest import BROKER_IS_CONNECTED, BROKER_IS_NOT_CONNECTED, WARNINGS_AS_ERRORS
from tests.stub.driver import StubConsumer


async def test_warning_on_del_of_connected_broker(stub_broker_driver: BrokerDriver) -> None:
    broker = Broker(nullcontext(stub_broker_driver))

    await broker.connect()

    with pytest.warns(RuntimeWarning, match="broker was not disconnected properly"):
        del broker


@WARNINGS_AS_ERRORS
async def test_no_warning_on_del_of_disconnected_broker(stub_broker_driver: BrokerDriver) -> None:
    broker = Broker(nullcontext(stub_broker_driver))

    await broker.connect()
    await broker.disconnect()

    del broker


@BROKER_IS_NOT_CONNECTED
def test_not_connected_broker_to_str_contains_appropriate_info(stub_broker: Broker) -> None:
    assert "is_connected=False" in str(stub_broker)
    assert "driver=None" in str(stub_broker)
    assert repr(stub_broker)


@BROKER_IS_CONNECTED
def test_connected_broker_to_str_contains_appropriate_info(stub_broker: Broker) -> None:
    assert "is_connected=True" in str(stub_broker)
    assert "driver" in str(stub_broker)
    assert "driver=None" not in str(stub_broker)
    assert repr(stub_broker)


@BROKER_IS_NOT_CONNECTED
async def test_broker_connect_sets_is_connected(stub_broker: Broker) -> None:
    await stub_broker.connect()

    assert stub_broker.is_connected


@BROKER_IS_CONNECTED
async def test_connected_broker_can_provide_publisher(stub_broker: Broker) -> None:
    async with stub_broker.publisher():
        pass


@BROKER_IS_NOT_CONNECTED
async def test_not_connected_broker_cant_provide_publisher(stub_broker: Broker) -> None:
    with pytest.raises(BrokerIsNotConnectedError):
        async with stub_broker.publisher():
            pass


@BROKER_IS_CONNECTED
def test_connected_broker_can_get_publisher_builder(stub_broker: Broker) -> None:
    stub_broker.build_publisher()


@BROKER_IS_CONNECTED
async def test_connected_broker_can_register_consumer(
    stub_broker: Broker,
    stub_consumer: StubConsumer,
    stub_binding_options: BindingOptions,
) -> None:
    async with stub_broker.consumer(stub_consumer, stub_binding_options):
        pass


@BROKER_IS_NOT_CONNECTED
async def test_not_connected_broker_cant_register_consumer(
    stub_broker: Broker,
    stub_consumer: StubConsumer,
    stub_binding_options: BindingOptions,
) -> None:
    with pytest.raises(BrokerIsNotConnectedError):
        async with stub_broker.consumer(stub_consumer, stub_binding_options):
            pass


@BROKER_IS_CONNECTED
def test_connected_broker_can_get_consumer_builder(stub_broker: Broker) -> None:
    stub_broker.build_consumer()
