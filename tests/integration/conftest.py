import typing as t

import pytest
from protomq.abc import Serializer
from protomq.caller import UnaryUnaryCaller
from protomq.client import Client
from protomq.options import BindingOptions, ExchangeOptions, PublisherOptions
from protomq.protobuf.serializer import ProtobufSerializer
from protomq.server import Server

from tests.stub import greetings_pb2
from tests.stub.greeting import consume_greet, do_fail
from tests.stub.queue import create_queue_options


@pytest.fixture()
def rabbitmq_url() -> str:
    return "amqp://guest:guest@localhost:5672/"


@pytest.fixture()
async def server(rabbitmq_url: str) -> t.AsyncIterator[Server]:
    async with Server.connect(rabbitmq_url) as server:
        yield server


@pytest.fixture()
async def running_server(server: Server) -> t.AsyncIterator[Server]:
    try:
        await server.start()

        yield server

    finally:
        await server.stop()


@pytest.fixture()
async def client(rabbitmq_url: str) -> t.AsyncIterator[Client]:
    async with Client.connect(rabbitmq_url) as session:
        yield session


@pytest.fixture()
def exchange_test_options() -> ExchangeOptions:
    return ExchangeOptions(
        name="test",
    )


@pytest.fixture()
def greeting_routing_key() -> str:
    return "greeting/SayHello"


@pytest.fixture()
def greeting_do_fail_key() -> str:
    return "greeting/DoFail"


@pytest.fixture()
def greeting_say_hello_serializer() -> Serializer[greetings_pb2.GreetingRequest, greetings_pb2.GreetingResponse]:
    return ProtobufSerializer(
        request_type=greetings_pb2.GreetingRequest,
        response_type=greetings_pb2.GreetingResponse,
    )


@pytest.fixture()
def greeting_say_hello_consumer(
    server: Server,
    exchange_test_options: ExchangeOptions,
    greeting_routing_key: str,
    greeting_say_hello_serializer: Serializer[greetings_pb2.GreetingRequest, greetings_pb2.GreetingResponse],
) -> None:
    server.register_unary_unary_consumer(
        func=consume_greet,
        serializer=greeting_say_hello_serializer,
        bindings=BindingOptions(
            exchange=exchange_test_options,
            binding_keys=[greeting_routing_key],
            queue=create_queue_options(consume_greet),
        ),
    )


@pytest.fixture()
def greeting_do_fail_consumer(
    server: Server,
    exchange_test_options: ExchangeOptions,
    greeting_do_fail_key: str,
    greeting_say_hello_serializer: Serializer[greetings_pb2.GreetingRequest, greetings_pb2.GreetingResponse],
) -> None:
    server.register_unary_unary_consumer(
        func=do_fail,
        serializer=greeting_say_hello_serializer,
        bindings=BindingOptions(
            exchange=exchange_test_options,
            binding_keys=[greeting_do_fail_key],
            queue=create_queue_options(do_fail),
        ),
    )


@pytest.fixture()
async def greeting_say_hello_caller(
    client: Client,
    exchange_test_options: ExchangeOptions,
    greeting_routing_key: str,
    greeting_say_hello_serializer: Serializer[greetings_pb2.GreetingRequest, greetings_pb2.GreetingResponse],
) -> t.AsyncIterator[UnaryUnaryCaller[greetings_pb2.GreetingRequest, greetings_pb2.GreetingResponse]]:
    async with client.register_unary_unary_caller(
        routing_key=greeting_routing_key,
        publisher=PublisherOptions(
            name=exchange_test_options.name,
        ),
        serializer=greeting_say_hello_serializer,
    ) as caller:
        yield caller


@pytest.fixture()
async def greeting_do_fail_caller(
    client: Client,
    exchange_test_options: ExchangeOptions,
    greeting_do_fail_key: str,
    greeting_say_hello_serializer: Serializer[greetings_pb2.GreetingRequest, greetings_pb2.GreetingResponse],
) -> t.AsyncIterator[UnaryUnaryCaller[greetings_pb2.GreetingRequest, greetings_pb2.GreetingResponse]]:
    async with client.register_unary_unary_caller(
        routing_key=greeting_do_fail_key,
        publisher=PublisherOptions(
            name=exchange_test_options.name,
        ),
        serializer=greeting_say_hello_serializer,
    ) as caller:
        yield caller
