import pytest

from brokrpc.message import Message, create_message
from brokrpc.serializer.json import JSONSerializer


@pytest.mark.parametrize("obj", [42, "hello, world", {"foo": "bar"}])
def test_dump_load_ok(serializer: JSONSerializer, message: Message[object]) -> None:
    loaded = serializer.decode_message(serializer.encode_message(message))

    assert loaded.body == message.body


@pytest.fixture
def serializer() -> JSONSerializer:
    return JSONSerializer()


@pytest.fixture
def message(obj: object) -> Message[object]:
    return create_message(body=obj, routing_key="test-json-serializer")
