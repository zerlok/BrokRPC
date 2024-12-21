import pytest
from pydantic import BaseModel

from brokrpc.message import AppMessage, Message
from brokrpc.serializer.pydantic import PydanticSerializer
from tests.stub.pydantic import FooModel


@pytest.mark.parametrize("obj", [FooModel(num=42, s="spam", bar=FooModel.Bar(str2int={"eggs": 59}))])
def test_dump_load_ok[T: BaseModel](serializer: PydanticSerializer[T], message: Message[T]) -> None:
    loaded = serializer.load_message(serializer.dump_message(message))

    assert loaded.body == message.body


@pytest.fixture
def serializer[T: BaseModel](obj: T) -> PydanticSerializer[T]:
    return PydanticSerializer(type(obj))


@pytest.fixture
def message[T: BaseModel](obj: T, stub_routing_key: str) -> Message[T]:
    return AppMessage(body=obj, routing_key=stub_routing_key)
