import builtins
import google.protobuf.message
import typing

class ConsumerOptions(google.protobuf.message.Message):

    def __init__(self, *, fading: typing.Optional[builtins.bool]=None) -> None:...

    @builtins.property
    def fading(self) -> builtins.bool:...

    def HasField(self, field_name: typing.Literal['fading']) -> builtins.bool:...

    def WhichOneof(self, oneof_group: typing.NoReturn) -> typing.NoReturn:...