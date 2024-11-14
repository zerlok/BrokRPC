# import pytest
# from brokrpc.rpc.caller import UnaryUnaryCaller
# from brokrpc.rpc.exception import CallError
# from brokrpc.rpc.server import Server
#
# from tests.stub import greetings_pb2
# from tests.stub.greeting import consume_greet
#
#
# @pytest.mark.parametrize("username", ["admin", "John", "Kate"])
# async def test_greeting_say_hello_caller_and_greet_consumer_results_are_same(
#     greeting_say_hello_consumer: object,
#     greeting_say_hello_caller: UnaryUnaryCaller[greetings_pb2.GreetingRequest, greetings_pb2.GreetingResponse],
#     running_server: Server,
#     username: str,
# ) -> None:
#     request = greetings_pb2.GreetingRequest(name=username)
#     assert (await greeting_say_hello_caller.request(request)) == (await consume_greet(request))
#
#
# async def test_greeting_say_hello_caller_fails(
#     greeting_do_fail_consumer: object,
#     greeting_do_fail_caller: UnaryUnaryCaller[greetings_pb2.GreetingRequest, greetings_pb2.GreetingResponse],
#     running_server: Server,
# ) -> None:
#     with pytest.raises(CallError):
#         await greeting_do_fail_caller.request(greetings_pb2.GreetingRequest(name="some error"))
