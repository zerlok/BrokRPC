import builtins
import google.protobuf.message
import typing

class Void(google.protobuf.message.Message):
    """Special type for marking methods that doesn't respond to RPC requests. Designed to be used in event driven
 architecture (fire & forget approach)."""

    def __init__(self) -> None:...

    def HasField(self, field_name: typing.NoReturn) -> typing.NoReturn:...

    def WhichOneof(self, oneof_group: typing.NoReturn) -> typing.NoReturn:...

class ConsumerOptions(google.protobuf.message.Message):

    def __init__(self, *, fading: typing.Optional[builtins.bool]=None) -> None:...

    @builtins.property
    def fading(self) -> builtins.bool:
        """If set, consumer will unbind the queue and consume all last messages in the queue."""
        ...

    def HasField(self, field_name: typing.Literal['fading']) -> builtins.bool:...

    def WhichOneof(self, oneof_group: typing.NoReturn) -> typing.NoReturn:...