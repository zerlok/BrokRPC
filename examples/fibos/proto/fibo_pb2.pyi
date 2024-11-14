from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor
EXCHANGE_FIELD_NUMBER: _ClassVar[int]
exchange: _descriptor.FieldDescriptor

class FiboRequest(_message.Message):
    __slots__ = ("n",)
    N_FIELD_NUMBER: _ClassVar[int]
    n: int
    def __init__(self, n: _Optional[int] = ...) -> None: ...

class FiboResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: int
    def __init__(self, result: _Optional[int] = ...) -> None: ...
