import json

import pytest
from google.protobuf.empty_pb2 import Empty
from google.protobuf.message import Message as ProtobufMessage

from brokrpc.message import Message, create_message
from brokrpc.model import SerializerLoadError
from brokrpc.serializer.protobuf import JSONProtobufSerializer, ProtobufSerializer
from tests.stub.proto.greeting_pb2 import GreetingRequest


@pytest.mark.parametrize(
    (
        "serializer",
        "body",
    ),
    [
        pytest.param(
            ProtobufSerializer(Empty),
            Empty(),
        ),
        pytest.param(
            ProtobufSerializer(GreetingRequest),
            GreetingRequest(name="Jack"),
        ),
        pytest.param(
            JSONProtobufSerializer(GreetingRequest),
            json.dumps({"name": "Jack"}).encode(),
        ),
    ],
)
def test_dump_load_ok[T: ProtobufMessage](
    serializer: ProtobufSerializer[T],
    message: Message[T],
) -> None:
    loaded = serializer.decode_message(serializer.encode_message(message))

    assert loaded.body == message.body


@pytest.mark.parametrize(
    (
        "serializer",
        "message",
        "expected_error",
        "expected_message",
    ),
    [
        pytest.param(
            ProtobufSerializer(Empty),
            create_message(body=b"""{"foo":"bar"}""", content_type="application/json", routing_key="test"),
            SerializerLoadError,
            "invalid content type",
        ),
        pytest.param(
            ProtobufSerializer(GreetingRequest),
            create_message(
                body=Empty().SerializeToString(),
                content_type="application/protobuf",
                message_type=Empty.DESCRIPTOR.full_name,
                routing_key="test",
            ),
            SerializerLoadError,
            "invalid message type",
        ),
        pytest.param(
            ProtobufSerializer(GreetingRequest),
            create_message(
                body=b"some invalid protobuf binary",
                content_type="application/protobuf",
                message_type=GreetingRequest.DESCRIPTOR.full_name,
                routing_key="test",
            ),
            SerializerLoadError,
            "can't decode protobuf message",
        ),
    ],
)
def test_load_error[T: ProtobufMessage](
    serializer: ProtobufSerializer[T],
    message: Message[bytes],
    expected_error: type[Exception],
    expected_message: str,
) -> None:
    with pytest.raises(expected_error, match=expected_message):
        serializer.decode_message(message)


@pytest.fixture
def message[T](body: T) -> Message[T]:
    return create_message(body=body, routing_key="test-pb2-serializer")
