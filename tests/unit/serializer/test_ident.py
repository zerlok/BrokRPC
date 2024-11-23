import pytest

from brokrpc.serializer.ident import IdentSerializer


@pytest.mark.parametrize("obj", [object(), 42, "hello, world"])
def test_dump_ok(serializer: IdentSerializer[object], obj: object) -> None:
    dumped = serializer.dump_message(obj)

    assert dumped is obj


@pytest.mark.parametrize("obj", [object(), 42, "hello, world"])
def test_load_ok(serializer: IdentSerializer[object], obj: object) -> None:
    loaded = serializer.load_message(obj)

    assert loaded is obj


@pytest.fixture
def serializer() -> IdentSerializer[object]:
    return IdentSerializer[object]()
