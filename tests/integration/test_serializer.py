import pytest
from pydantic import BaseModel

from brokrpc.message import Message, create_message
from brokrpc.serializer.json import JSONSerializer
from brokrpc.serializer.pydantic import PydanticSerializer
from tests.stub.pydantic import FooModel


@pytest.mark.parametrize("obj", [FooModel(num=42, s="spam", bar=FooModel.Bar(str2int={"eggs": 59}))])
def test_pydantic_dump_json_load_ok[T: BaseModel](
    pydantic_serializer: PydanticSerializer[T],
    json_serializer: JSONSerializer,
    message: Message[T],
    obj: BaseModel,
) -> None:
    loaded = json_serializer.decode_message(pydantic_serializer.encode_message(message))

    assert loaded.body == obj.model_dump(mode="json", by_alias=True, exclude_unset=True, exclude_none=True)


@pytest.fixture
def pydantic_serializer[T: BaseModel](obj: T) -> PydanticSerializer[T]:
    return PydanticSerializer(type(obj))


@pytest.fixture
def message[T: BaseModel](obj: T, stub_routing_key: str) -> Message[T]:
    return create_message(body=obj, routing_key=stub_routing_key)
