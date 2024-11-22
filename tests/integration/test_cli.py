import asyncio
import typing as t
from argparse import ArgumentParser

import pytest

from brokrpc.cli import CLIOptions, build_parser, run
from brokrpc.options import BrokerOptions
from tests.stub.stream import InMemoryStream


@pytest.mark.parametrize("body", [b"test-cli-body"])
async def test_publisher_consumer_messaging(
    consumer: asyncio.Task[int],
    publisher: asyncio.Task[int],
    publisher_options: CLIOptions,
    consumer_options: CLIOptions,
    done: asyncio.Event,
    body: bytes,
) -> None:
    await asyncio.sleep(1.0)  # wait for consumer is registered
    await publisher_options.input.write(body)
    await publisher_options.input.close()

    await asyncio.sleep(1.0)  # wait for message consumption
    done.set()

    await asyncio.gather(publisher, consumer)

    publisher_logs = await publisher_options.err.readlines()
    consumer_output = await consumer_options.output.readlines()

    assert publisher_logs == [b"sent"]
    assert consumer_output == [body]


@pytest.fixture
def publisher(event_loop: asyncio.AbstractEventLoop, publisher_options: CLIOptions) -> t.Iterator[asyncio.Task[int]]:
    task = event_loop.create_task(run(publisher_options))
    try:
        yield task

    finally:
        if not task.done():
            task.cancel()


@pytest.fixture
def consumer(event_loop: asyncio.AbstractEventLoop, consumer_options: CLIOptions) -> t.Iterator[asyncio.Task[int]]:
    task = event_loop.create_task(run(consumer_options))
    try:
        yield task

    finally:
        if not task.done():
            task.cancel()


@pytest.fixture
def parser() -> ArgumentParser:
    parser = build_parser()

    # NOTE: raise exceptions instead of `sys.exit`
    parser.exit_on_error = False  # type: ignore[attr-defined]

    return parser


@pytest.fixture
def routing_key() -> str:
    return "test-cli-messaging"


@pytest.fixture
def exchange_name() -> str:
    return "test-cli"


@pytest.fixture
def publisher_options(
    rabbitmq_options: BrokerOptions,
    parser: ArgumentParser,
    routing_key: str,
    exchange_name: str,
    done: asyncio.Event,
) -> CLIOptions:
    options = parser.parse_args(
        [
            str(rabbitmq_options.url),
            routing_key,
            "--publisher",
            f"--exchange={exchange_name}",
        ],
        namespace=CLIOptions(),
    )

    options.input = InMemoryStream()
    options.output = InMemoryStream()
    options.err = InMemoryStream()
    options.done = done

    return options


@pytest.fixture
def consumer_options(
    rabbitmq_options: BrokerOptions,
    parser: ArgumentParser,
    routing_key: str,
    exchange_name: str,
    done: asyncio.Event,
) -> CLIOptions:
    options = parser.parse_args(
        [
            str(rabbitmq_options.url),
            routing_key,
            "--consumer",
            f"--exchange={exchange_name}",
        ],
        namespace=CLIOptions(),
    )

    options.input = InMemoryStream()
    options.output = InMemoryStream()
    options.err = InMemoryStream()
    options.done = done

    return options


@pytest.fixture
def done() -> asyncio.Event:
    return asyncio.Event()
