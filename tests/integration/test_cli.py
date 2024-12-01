import asyncio
from asyncio import TaskGroup

import pytest

from brokrpc.abc import Serializer
from brokrpc.broker import Broker
from brokrpc.cli import AsyncStream, Console, ConsumerApp, PublisherApp
from brokrpc.message import BinaryMessage
from brokrpc.options import BindingOptions, PublisherOptions
from tests.stub.stream import InMemoryStream


@pytest.mark.parametrize("body", [b"test-cli-body"])
async def test_publisher_consumer_messaging(
    *,
    consumer_app: ConsumerApp,
    consumer_stdin: AsyncStream,
    consumer_stdout: AsyncStream,
    consumer_stderr: AsyncStream,
    publisher_app: PublisherApp,
    publisher_stdin: AsyncStream,
    publisher_stderr: AsyncStream,
    body: bytes,
) -> None:
    async with TaskGroup() as tg:
        tg.create_task(consumer_app.run())
        tg.create_task(publisher_app.run())

        # wait for consumer is registered
        await asyncio.sleep(1.0)

        # publish message
        await publisher_stdin.write(body + b"\n")
        await publisher_stdin.flush()
        await publisher_stdin.close()

        # wait for message consumption
        await asyncio.sleep(1.0)
        await consumer_stdin.close()

    publisher_logs = await publisher_stderr.readlines()
    consumer_output = await consumer_stdout.readlines()
    consumer_logs = await consumer_stderr.readlines()

    assert (publisher_logs, consumer_output, consumer_logs) == ([b"message sent"], [body], [b"message consumed"])


@pytest.fixture
def publisher_app(
    *,
    rabbitmq_broker: Broker,
    publisher_stdin: AsyncStream,
    publisher_stdout: AsyncStream,
    publisher_stderr: AsyncStream,
    stub_publisher_options: PublisherOptions,
    stub_routing_key: str,
    raw_encoder: Serializer[BinaryMessage, BinaryMessage],
) -> PublisherApp:
    return PublisherApp(
        broker=rabbitmq_broker,
        options=stub_publisher_options,
        console=Console(publisher_stdin, publisher_stdout, publisher_stderr),
        routing_key=stub_routing_key,
        serializer=raw_encoder,
    )


@pytest.fixture
def consumer_app(
    *,
    rabbitmq_broker: Broker,
    consumer_stdin: AsyncStream,
    consumer_stdout: AsyncStream,
    consumer_stderr: AsyncStream,
    stub_binding_options: BindingOptions,
    raw_decoder: Serializer[bytes, BinaryMessage],
) -> ConsumerApp:
    return ConsumerApp(
        broker=rabbitmq_broker,
        options=stub_binding_options,
        serializer=raw_decoder,
        console=Console(consumer_stdin, consumer_stdout, consumer_stderr),
    )


@pytest.fixture
def raw_encoder() -> Serializer[BinaryMessage, BinaryMessage]:
    class RawEncoder(Serializer[BinaryMessage, BinaryMessage]):
        def dump_message(self, message: BinaryMessage) -> BinaryMessage:
            return message

        def load_message(self, message: BinaryMessage) -> BinaryMessage:
            return message

    return RawEncoder()


@pytest.fixture
def raw_decoder() -> Serializer[bytes, BinaryMessage]:
    class RawDecoder(Serializer[bytes, BinaryMessage]):
        def dump_message(self, message: bytes) -> BinaryMessage:
            raise NotImplementedError

        def load_message(self, message: BinaryMessage) -> bytes:
            return message.body

    return RawDecoder()


@pytest.fixture
def publisher_stdin() -> AsyncStream:
    return InMemoryStream()


@pytest.fixture
def publisher_stdout() -> AsyncStream:
    return InMemoryStream()


@pytest.fixture
def publisher_stderr() -> AsyncStream:
    return InMemoryStream()


@pytest.fixture
def consumer_stdin() -> AsyncStream:
    return InMemoryStream()


@pytest.fixture
def consumer_stdout() -> AsyncStream:
    return InMemoryStream()


@pytest.fixture
def consumer_stderr() -> AsyncStream:
    return InMemoryStream()
