from tests.stub import greetings_pb2


class GreetingError(Exception):
    pass


async def consume_greet(request: greetings_pb2.GreetingRequest) -> greetings_pb2.GreetingResponse:
    return greetings_pb2.GreetingResponse(message=f"Greetings, {request.name}")


async def do_fail(request: greetings_pb2.GreetingRequest) -> greetings_pb2.GreetingResponse:
    raise GreetingError(request)
