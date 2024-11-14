import asyncio

from brokrpc.options import ConsumerOptions
from brokrpc.rpc.server import Server, run_server_until_termination

from examples.fibos.proto import fibo_pb2
from examples.fibos.proto.fibo_pb2_brokrpc import FiboConsumer, add_fibo_consumer_to_server


# presentation layer
class FiboConsumerImpl(FiboConsumer):
    @ConsumerOptions(
        prefetch_count=100,
        retry_on_error=(ValueError,),
    )
    async def count_fibo(self, request: fibo_pb2.FiboRequest) -> fibo_pb2.FiboResponse:
        result = do_fibo(request.n)
        return fibo_pb2.FiboResponse(result=result)


# domain layer
def do_fibo(n: int) -> int:
    a, b = 0, 1

    if n <= 0:
        return a

    if n == 1:
        return b

    for _ in range(2, n + 1):
        a, b = b, a + b

    return b


async def main() -> None:
    server: Server

    async with Server.connect("amqp://guest:guest@localhost:5672/") as server:
        add_fibo_consumer_to_server(FiboConsumerImpl(), server)

        await run_server_until_termination(server)


if __name__ == "__main__":
    asyncio.run(main())
