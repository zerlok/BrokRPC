import pytest
from pydantic import BaseModel

from brokrpc.message import AppMessage, Message
from brokrpc.serializer.json import JSONSerializer
from brokrpc.serializer.pydantic import PydanticSerializer
from tests.stub.pydantic import FooModel


@pytest.mark.parametrize("obj", [FooModel(num=42, s="spam", bar=FooModel.Bar(str2int={"eggs": 59}))])
def test_pydantic_dump_json_load_ok(
    pydantic_serializer: PydanticSerializer,
    json_serializer: JSONSerializer,
    message: Message[object],
    obj: BaseModel,
) -> None:
    loaded = json_serializer.load_message(pydantic_serializer.dump_message(message))

    assert loaded.body == obj.model_dump(mode="json", by_alias=True, exclude_unset=True, exclude_none=True)


@pytest.fixture
def pydantic_serializer(obj: BaseModel) -> PydanticSerializer:
    return PydanticSerializer(type(obj))


@pytest.fixture
def message(obj: object, stub_routing_key: str) -> Message[object]:
    return AppMessage(body=obj, routing_key=stub_routing_key)
