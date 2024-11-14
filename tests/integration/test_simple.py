import asyncio
import typing as t
from dataclasses import asdict

import pytest
from _pytest.fixtures import SubRequest
from brokrpc.abc import BinaryPublisher, Publisher, Serializer
from brokrpc.broker import Broker
from brokrpc.message import AppMessage, Message
from brokrpc.options import BindingOptions, ExchangeOptions, PublisherOptions, QueueOptions
from brokrpc.rpc.abc import Caller, CallerSerializer, RPCSerializer
from brokrpc.rpc.client import Client
from brokrpc.rpc.server import Server

from tests.stub.simple import ReceiveWaiter, ReceiveWaiterConsumer, ReceiveWaiterHandler


async def test_consumer_receives_published_message(
    receive_waiter: ReceiveWaiter,
    consumer: object,
    publisher: Publisher[object, object],
    message: Message[object],
) -> None:
    await publisher.publish(message)
    received_message = await receive_waiter.wait()

    assert received_message.body == message.body


async def test_rpc_handles_request(
    receive_waiter: ReceiveWaiter,
    handler: ReceiveWaiterHandler,
    caller: Caller[object, object],
    running_rpc_server: Server,
    content: object,
) -> None:
    response = await caller.invoke(content)

    assert response.body == receive_waiter.process_value(content)


@pytest.fixture()
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
        pytest.param(ReceiveWaiter.handler_async),
        # TODO: support ReceiveWaiter.handler_sync & ReceiveWaiter.handler_impl
    ],
)
def receive_waiter_handler(
    request: SubRequest,
    receive_waiter: ReceiveWaiter,
) -> ReceiveWaiterHandler:
    factory = t.cast(t.Callable[[ReceiveWaiter], ReceiveWaiterHandler], request.param)
    return factory(receive_waiter)


@pytest.fixture()
async def consumer(
    rabbitmq_broker: Broker,
    receive_waiter_consumer: ReceiveWaiterConsumer,
    json_serializer: Serializer[Message[object], Message[bytes]],
    binding_options: BindingOptions,
) -> t.AsyncIterator[ReceiveWaiterConsumer]:
    async with rabbitmq_broker.consumer(receive_waiter_consumer, binding_options, serializer=json_serializer):
        yield receive_waiter_consumer


@pytest.fixture()
async def publisher(
    rabbitmq_broker: Broker,
    json_serializer: Serializer[Message[object], Message[bytes]],
    publisher_options: PublisherOptions,
) -> t.AsyncIterator[BinaryPublisher]:
    async with rabbitmq_broker.publisher(publisher_options, serializer=json_serializer) as pub:
        yield pub


@pytest.fixture()
def handler(
    routing_key: str,
    binding_options: BindingOptions,
    rpc_server: Server,
    json_serializer: RPCSerializer[object, object],
    receive_waiter_handler: ReceiveWaiterHandler,
) -> ReceiveWaiterHandler:
    rpc_server.register_unary_unary_handler(
        func=receive_waiter_handler,
        routing_key=routing_key,
        serializer=json_serializer,
        exchange=binding_options.exchange,
        queue=binding_options.queue,
    )

    return receive_waiter_handler


@pytest.fixture()
async def caller(
    exchange: ExchangeOptions,
    routing_key: str,
    rpc_client: Client,
    json_serializer: CallerSerializer[object, object],
) -> t.AsyncIterator[Caller[object, object]]:
    async with rpc_client.unary_unary_caller(
        routing_key=routing_key,
        serializer=json_serializer,
        exchange=exchange,
    ) as caller:
        yield caller


@pytest.fixture()
def exchange() -> ExchangeOptions:
    return ExchangeOptions(name="simple-test", auto_delete=True)


@pytest.fixture()
def publisher_options(exchange: ExchangeOptions) -> PublisherOptions:
    return PublisherOptions(**asdict(exchange))


@pytest.fixture()
def binding_options(publisher_options: PublisherOptions, routing_key: str) -> BindingOptions:
    return BindingOptions(
        exchange=publisher_options,
        binding_keys=(routing_key,),
        queue=QueueOptions(
            auto_delete=True,
        ),
    )


@pytest.fixture()
def routing_key() -> str:
    return "test-simple"


@pytest.fixture()
def message(content: object, routing_key: str) -> Message[object]:
    return AppMessage(body=content, routing_key=routing_key)


@pytest.fixture(
    params=[
        pytest.param("John"),
        pytest.param("Bob"),
        pytest.param({"foo": "bar"}),
    ]
)
def content(request: SubRequest) -> object:
    return request.param
