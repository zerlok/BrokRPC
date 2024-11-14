import asyncio

from brokrpc.rpc.client import Client

from examples.fibos.proto import fibo_pb2
from examples.fibos.proto.fibo_pb2_brokrpc import FiboClient


async def main() -> None:
    session: Client
    client: FiboClient

    async with (
        Client.connect("amqp://guest:guest@localhost:5672/") as session,
        FiboClient.create(session) as client,
    ):
        request = fibo_pb2.FiboRequest(n=15)
        response = await client.count_fibo(request)

        print(f"fibo({request.n}) = {response.result}")


if __name__ == "__main__":
    asyncio.run(main())
