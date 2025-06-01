import asyncio
import typing as t
from dataclasses import asdict

import pytest
from _pytest.fixtures import SubRequest

from brokrpc.abc import BinaryPublisher, Publisher, Serializer
from brokrpc.broker import Broker
from brokrpc.message import Message, create_message
from brokrpc.options import BindingOptions, ExchangeOptions, PublisherOptions, QueueOptions
from brokrpc.rpc.abc import Caller, RPCSerializer
from brokrpc.rpc.client import Client
from brokrpc.rpc.server import Server
from tests.stub.proto.greeting_pb2 import GreetingRequest, GreetingResponse
from tests.stub.simple import GreetingHandler, ReceiveWaiter, ReceiveWaiterConsumer


async def test_consumer_receives_published_message(
    receive_waiter: ReceiveWaiter,
    consumer: object,
    publisher: Publisher[object, object],
    json_message: Message[object],
) -> None:
    await publisher.publish(json_message)
    received_message = await receive_waiter.wait()

    assert received_message.body == json_message.body


async def test_rpc_handles_request(
    receive_waiter: ReceiveWaiter,
    greeting_handler: GreetingHandler,
    caller: Caller[GreetingRequest, GreetingResponse],
    running_rpc_server: Server,
    protobuf_request: GreetingRequest,
) -> None:
    response = await caller.invoke(protobuf_request)

    assert response.body.result == receive_waiter.process_value(protobuf_request.name)


@pytest.fixture
def receive_waiter(event_loop: asyncio.AbstractEventLoop) -> ReceiveWaiter:
    return ReceiveWaiter(event_loop)


@pytest.fixture(
    params=[
        pytest.param(ReceiveWaiter.consumer_sync),
        pytest.param(ReceiveWaiter.consumer_async),
        pytest.param(ReceiveWaiter.consumer_impl),
    ],
)
def receive_waiter_consumer(
    request: SubRequest,
    receive_waiter: ReceiveWaiter,
) -> ReceiveWaiterConsumer:
    factory = t.cast(t.Callable[[ReceiveWaiter], ReceiveWaiterConsumer], request.param)
    return factory(receive_waiter)


@pytest.fixture(
    params=[
        pytest.param(ReceiveWaiter.handler_sync),
        pytest.param(ReceiveWaiter.handler_async),
        pytest.param(ReceiveWaiter.handler_impl),
    ],
)
def receive_waiter_handler(
    request: SubRequest,
    receive_waiter: ReceiveWaiter,
) -> GreetingHandler:
    factory = t.cast(t.Callable[[ReceiveWaiter], GreetingHandler], request.param)
    return factory(receive_waiter)


@pytest.fixture
async def consumer(
    rabbitmq_broker: Broker,
    receive_waiter_consumer: ReceiveWaiterConsumer,
    json_serializer: Serializer[Message[object], Message[bytes]],
    binding_options: BindingOptions,
) -> t.AsyncIterator[ReceiveWaiterConsumer]:
    async with rabbitmq_broker.consumer(receive_waiter_consumer, binding_options, serializer=json_serializer):
        yield receive_waiter_consumer


@pytest.fixture
async def publisher(
    rabbitmq_broker: Broker,
    json_serializer: Serializer[Message[object], Message[bytes]],
    publisher_options: PublisherOptions,
) -> t.AsyncIterator[BinaryPublisher]:
    async with rabbitmq_broker.publisher(publisher_options, serializer=json_serializer) as pub:
        yield pub


@pytest.fixture
def greeting_handler(
    routing_key: str,
    binding_options: BindingOptions,
    rpc_server: Server,
    rpc_greeting_serializer: RPCSerializer[GreetingRequest, GreetingResponse],
    receive_waiter_handler: GreetingHandler,
) -> GreetingHandler:
    rpc_server.register_unary_unary_handler(
        func=receive_waiter_handler,
        routing_key=routing_key,
        serializer=rpc_greeting_serializer,
        exchange=binding_options.exchange,
        queue=binding_options.queue,
    )

    return receive_waiter_handler


@pytest.fixture
async def caller(
    exchange: ExchangeOptions,
    routing_key: str,
    rpc_client: Client,
    rpc_greeting_serializer: RPCSerializer[GreetingRequest, GreetingResponse],
) -> t.AsyncIterator[Caller[GreetingRequest, GreetingResponse]]:
    async with rpc_client.unary_unary_caller(
        routing_key=routing_key,
        serializer=rpc_greeting_serializer,
        exchange=exchange,
    ) as caller:
        yield caller


@pytest.fixture
def exchange() -> ExchangeOptions:
    return ExchangeOptions(name="simple-test", auto_delete=True)


@pytest.fixture
def publisher_options(exchange: ExchangeOptions) -> PublisherOptions:
    return PublisherOptions(**asdict(exchange))


@pytest.fixture
def binding_options(publisher_options: PublisherOptions, routing_key: str) -> BindingOptions:
    return BindingOptions(
        exchange=publisher_options,
        binding_keys=(routing_key,),
        queue=QueueOptions(
            auto_delete=True,
        ),
    )


@pytest.fixture
def routing_key() -> str:
    return "test-simple"


@pytest.fixture
def json_message(json_content: object, routing_key: str) -> Message[object]:
    return create_message(body=json_content, routing_key=routing_key)


@pytest.fixture(
    params=[
        pytest.param("John"),
        pytest.param("Bob"),
        pytest.param({"foo": "bar"}),
    ]
)
def json_content(request: SubRequest) -> object:
    return request.param


@pytest.fixture(
    params=[
        pytest.param(GreetingRequest(name="John")),
        pytest.param(GreetingRequest(name="Bob")),
    ]
)
def protobuf_request(request: SubRequest) -> object:
    return request.param
