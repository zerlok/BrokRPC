import asyncio
from datetime import timedelta
from pprint import pprint

from protomq.broker import Broker
from protomq.middleware import RetryOnErrorConsumerMiddleware
from protomq.options import ExchangeOptions
from protomq.rpc.client import Client
from protomq.rpc.server import Server
from protomq.serializer.json import JSONSerializer


# define app RPC handler
async def handle_request(msg: object) -> str:
    return f"I greet you, {msg}"


async def main() -> None:
    # common AMQP & serialization settings
    routing_key = "test-greeting"
    serializer = JSONSerializer()

    # create broker, RPC server & RPC client
    broker = Broker(
        dsn="amqp://guest:guest@localhost:5672/",
        default_exchange=ExchangeOptions(name="simple-test-app"),
        default_consumer_middlewares=[RetryOnErrorConsumerMiddleware((Exception,), timedelta(seconds=3.0))],
    )
    server, client = Server(broker), Client(broker)

    # register RPC handler
    server.register_unary_unary_handler(
        handle_request,
        routing_key,
        serializer=serializer,
    )

    async with (
        # connect to broker
        broker,
        # start RPC server consumer
        server.run(),
        # get RPC caller
        client.unary_unary_caller(
            routing_key,
            serializer=serializer,
            # timeout=timedelta(seconds=10.0),
        ) as caller,
    ):
        # publish app message & receive RPC response
        response = await caller.invoke("John")
        pprint(response)  # noqa: T203


if __name__ == "__main__":
    asyncio.run(main())
