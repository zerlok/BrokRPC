import builtins
import google.protobuf.message
import typing

class GreetingRequest(google.protobuf.message.Message):

    def __init__(self, *, name: builtins.str) -> None:...

    @builtins.property
    def name(self) -> builtins.str:...

    def HasField(self, field_name: typing.NoReturn) -> typing.NoReturn:...

    def WhichOneof(self, oneof_group: typing.NoReturn) -> typing.NoReturn:...

class GreetingResponse(google.protobuf.message.Message):

    def __init__(self, *, result: builtins.str) -> None:...

    @builtins.property
    def result(self) -> builtins.str:...

    def HasField(self, field_name: typing.NoReturn) -> typing.NoReturn:...

    def WhichOneof(self, oneof_group: typing.NoReturn) -> typing.NoReturn:...