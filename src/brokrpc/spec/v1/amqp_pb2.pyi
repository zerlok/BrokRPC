import builtins
import enum
import google.protobuf.descriptor
import google.protobuf.descriptor_pb2
import google.protobuf.message
import pyprotostuben.protobuf.extension
import typing

class ExchangeType(enum.IntEnum):
    """All possible types of exchanges in AMQP"""
    EXCHANGE_TYPE_UNSPECIFIED = 0
    EXCHANGE_TYPE_DIRECT = 1
    EXCHANGE_TYPE_FANOUT = 2
    EXCHANGE_TYPE_TOPIC = 3
    EXCHANGE_TYPE_HEADER = 4

class ArgumentListValue(google.protobuf.message.Message):

    def __init__(self, *, items: typing.Sequence[ArgumentValue]) -> None:...

    @builtins.property
    def items(self) -> typing.Sequence[ArgumentValue]:...

    def HasField(self, field_name: typing.NoReturn) -> typing.NoReturn:...

    def WhichOneof(self, oneof_group: typing.NoReturn) -> typing.NoReturn:...
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

class ArgumentMapValue(google.protobuf.message.Message):

    def __init__(self, *, items: typing.Mapping[builtins.str, ArgumentValue]) -> None:...

    @builtins.property
    def items(self) -> typing.Mapping[builtins.str, ArgumentValue]:...

    def HasField(self, field_name: typing.NoReturn) -> typing.NoReturn:...

    def WhichOneof(self, oneof_group: typing.NoReturn) -> typing.NoReturn:...
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

class ArgumentValue(google.protobuf.message.Message):

    def __init__(self, *, null_value: typing.Optional[builtins.bool]=None, bool_value: typing.Optional[builtins.bool]=None, int_value: typing.Optional[builtins.int]=None, float_value: typing.Optional[builtins.float]=None, str_value: typing.Optional[builtins.str]=None, list_value: typing.Optional[ArgumentListValue]=None, map_value: typing.Optional[ArgumentMapValue]=None) -> None:...

    @builtins.property
    def null_value(self) -> builtins.bool:...

    @builtins.property
    def bool_value(self) -> builtins.bool:...

    @builtins.property
    def int_value(self) -> builtins.int:...

    @builtins.property
    def float_value(self) -> builtins.float:...

    @builtins.property
    def str_value(self) -> builtins.str:...

    @builtins.property
    def list_value(self) -> ArgumentListValue:...

    @builtins.property
    def map_value(self) -> ArgumentMapValue:...

    def HasField(self, field_name: typing.NoReturn) -> typing.NoReturn:...

    def WhichOneof(self, oneof_group: typing.Literal['value']) -> typing.Optional[typing.Literal['null_value', 'bool_value', 'int_value', 'float_value', 'str_value', 'list_value', 'map_value']]:...
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

class ExchangeOptions(google.protobuf.message.Message):
    """A set of attributes of exchange entity in AMQP"""

    def __init__(self, *, name: typing.Optional[builtins.str]=None, type: typing.Optional[ExchangeType]=None, durable: typing.Optional[builtins.bool]=None, auto_delete: typing.Optional[builtins.bool]=None, arguments: typing.Mapping[builtins.str, ArgumentValue]) -> None:...

    @builtins.property
    def name(self) -> builtins.str:...

    @builtins.property
    def type(self) -> ExchangeType:...

    @builtins.property
    def durable(self) -> builtins.bool:...

    @builtins.property
    def auto_delete(self) -> builtins.bool:...

    @builtins.property
    def arguments(self) -> typing.Mapping[builtins.str, ArgumentValue]:...

    def HasField(self, field_name: typing.Literal['name', 'type', 'durable', 'auto_delete']) -> builtins.bool:...

    def WhichOneof(self, oneof_group: typing.NoReturn) -> typing.NoReturn:...
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

class QueueOptions(google.protobuf.message.Message):
    """A set of attributes of queue entity in AMQP"""

    def __init__(self, *, name: typing.Optional[builtins.str]=None, durable: typing.Optional[builtins.bool]=None, exclusive: typing.Optional[builtins.bool]=None, auto_delete: typing.Optional[builtins.bool]=None, arguments: typing.Mapping[builtins.str, ArgumentValue]) -> None:...

    @builtins.property
    def name(self) -> builtins.str:...

    @builtins.property
    def durable(self) -> builtins.bool:...

    @builtins.property
    def exclusive(self) -> builtins.bool:...

    @builtins.property
    def auto_delete(self) -> builtins.bool:...

    @builtins.property
    def arguments(self) -> typing.Mapping[builtins.str, ArgumentValue]:...

    def HasField(self, field_name: typing.Literal['name', 'durable', 'exclusive', 'auto_delete']) -> builtins.bool:...

    def WhichOneof(self, oneof_group: typing.NoReturn) -> typing.NoReturn:...
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
exchange: typing.Final[pyprotostuben.protobuf.extension.ExtensionDescriptor[google.protobuf.descriptor_pb2.ServiceOptions, ExchangeOptions]]
queue: typing.Final[pyprotostuben.protobuf.extension.ExtensionDescriptor[google.protobuf.descriptor_pb2.MethodOptions, QueueOptions]]
DESCRIPTOR: google.protobuf.descriptor.FileDescriptor