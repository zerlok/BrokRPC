import asyncio
import typing as t

import pytest
from protomq.abc import RawPublisher
from protomq.broker import Broker
from protomq.model import Message, RawMessage
from protomq.options import BindingOptions, PublisherOptions

from tests.stub.simple import SimpleConsumer


async def test_simple_consumer_receives_message(
    consumer: SimpleConsumer,
    publisher: RawPublisher,
    message: RawMessage,
) -> None:
    await publisher.publish(message)
    received_message = await asyncio.wait_for(consumer.wait(), 5.0)

    assert received_message.body == message.body


@pytest.fixture()
async def consumer(
    rabbitmq_connection: Broker,
    binding_options: BindingOptions,
) -> t.AsyncIterator[SimpleConsumer]:
    consumer = SimpleConsumer()

    async with rabbitmq_connection.consumer(consumer, binding_options):
        yield consumer


@pytest.fixture()
async def publisher(
    rabbitmq_connection: Broker,
    publisher_options: PublisherOptions,
) -> t.AsyncIterator[RawPublisher]:
    async with rabbitmq_connection.publisher(publisher_options) as pub:
        yield pub


@pytest.fixture()
def publisher_options() -> PublisherOptions:
    return PublisherOptions(name="simple-test")


@pytest.fixture()
def binding_options(publisher_options: PublisherOptions, message: Message[object]) -> BindingOptions:
    return BindingOptions(exchange=publisher_options, binding_keys=(message.routing_key,))


@pytest.fixture()
def message() -> RawMessage:
    return Message(body=b"hello world", routing_key="test-simple")
