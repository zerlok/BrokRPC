import pytest
from brokrpc.abc import BrokerDriver
from brokrpc.broker import Broker, BrokerNotConnectedError
from brokrpc.options import BindingOptions

from tests.stub.driver import StubConsumer


def test_not_connected_broker_to_str_contains_appropriate_info(
    not_connected_broker: Broker,
) -> None:
    assert "is_connected=False" in str(not_connected_broker)
    assert "driver=None" in str(not_connected_broker)
    assert repr(not_connected_broker)


def test_connected_broker_to_str_contains_appropriate_info(
    broker_driver: BrokerDriver,
    connected_broker: Broker,
) -> None:
    assert "is_connected=True" in str(connected_broker)
    assert "driver" in str(connected_broker) and "driver=None" not in str(connected_broker)
    assert repr(connected_broker)


async def test_broker_connect_sets_is_connected(
    not_connected_broker: Broker,
) -> None:
    await not_connected_broker.connect()

    assert not_connected_broker.is_connected


async def test_not_connected_broker_cant_provide_publisher(
    not_connected_broker: Broker,
) -> None:
    with pytest.raises(BrokerNotConnectedError):
        async with not_connected_broker.publisher():
            pass


async def test_not_connected_broker_cant_register_consumer(
    not_connected_broker: Broker,
    stub_consumer: StubConsumer,
) -> None:
    with pytest.raises(BrokerNotConnectedError):
        async with not_connected_broker.consumer(stub_consumer, BindingOptions(binding_keys=("test",))):
            pass


@pytest.fixture()
def not_connected_broker(broker: Broker) -> Broker:
    assert not broker.is_connected
    return broker
