import asyncio
from datetime import timedelta

from brokrpc.broker import Broker
from brokrpc.middleware import RetryOnErrorConsumerMiddleware
from brokrpc.options import ExchangeOptions
from brokrpc.rpc.client import Client
from brokrpc.rpc.model import Request
from brokrpc.rpc.server import Server
from brokrpc.serializer.json import JSONSerializer


# define app RPC handler
async def handle_request(request: Request[object]) -> str:
    print(f"{request=!s}")
    print(f"{request.body=}")
    return f"I greet you, {request.body}"


async def main() -> None:
    # common AMQP & serialization settings
    routing_key = "test-greeting"
    serializer = JSONSerializer()

    # create broker, RPC server & RPC client
    broker = Broker(
        options="amqp://guest:guest@localhost:5672/",
        default_exchange=ExchangeOptions(name="simple-test-app"),
        default_consumer_middlewares=[RetryOnErrorConsumerMiddleware(timedelta(seconds=3.0), (Exception,))],
    )
    server, client = Server(broker), Client(broker)

    # register RPC handler
    server.register_unary_unary_handler(
        func=handle_request,
        routing_key=routing_key,
        serializer=serializer,
    )

    async with (
        # connect to broker
        broker,
        # start RPC server consumer
        server.run(),
        # get RPC caller
        client.unary_unary_caller(
            routing_key=routing_key,
            serializer=serializer,
            # timeout=timedelta(seconds=10.0),
        ) as caller,
    ):
        print(server)
        # publish app message & receive RPC response
        response = await caller.invoke("John")
        print(response)
        print(f"{response.body=}")


if __name__ == "__main__":
    asyncio.run(main())
