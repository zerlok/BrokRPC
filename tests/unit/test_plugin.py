import pytest

from brokrpc.plugin import Loader
from tests.stub.plugin.abc import Bar, Foo


class TestLoader:
    def test_load_class_ok(self, foo_int_loader: Loader[Foo[int]]) -> None:
        klass = foo_int_loader.load_class("tests.stub.plugin.impl:FooImpl")
        obj = klass()

        assert issubclass(klass, Foo)
        assert isinstance(obj, Foo)

    @pytest.mark.parametrize(
        ("entrypoint", "expected_error", "expected_message"),
        [
            pytest.param(
                "invalid-entrypoint",
                ValueError,
                "invalid entrypoint",
            ),
            pytest.param(
                "tests.stub.plugin.unknown_module:UnknownClass",
                ValueError,
                "invalid entrypoint module",
            ),
            pytest.param(
                "tests.stub.plugin.impl:UnknownClass",
                ValueError,
                "invalid entrypoint attribute",
            ),
            pytest.param(
                "tests.stub.plugin.impl:BarImpl",
                TypeError,
                "invalid class type",
            ),
        ],
    )
    def test_load_class_error(
        self,
        foo_int_loader: Loader[Foo[int]],
        entrypoint: str,
        expected_error: type[Exception],
        expected_message: str,
    ) -> None:
        with pytest.raises(expected_error, match=expected_message):
            foo_int_loader.load_class(entrypoint)

    def test_load_instance_ok(self, bar_loader: Loader[Bar]) -> None:
        obj1 = bar_loader.load_instance("tests.stub.plugin.impl:load_bar")
        obj2 = bar_loader.load_instance("tests.stub.plugin.impl:load_bar")

        assert isinstance(obj1, Bar)
        assert isinstance(obj2, Bar)
        assert obj1 is not obj2

    @pytest.mark.parametrize(
        ("entrypoint", "expected_error", "expected_message"),
        [
            pytest.param(
                "invalid-entrypoint",
                ValueError,
                "invalid entrypoint",
            ),
            pytest.param(
                "tests.stub.plugin.unknown_module:UnknownClass",
                ValueError,
                "invalid entrypoint module",
            ),
            pytest.param(
                "tests.stub.plugin.impl:UnknownClass",
                ValueError,
                "invalid entrypoint attribute",
            ),
            pytest.param(
                "tests.stub.plugin.impl:FOO_INT",
                TypeError,
                "invalid factory type",
            ),
            pytest.param(
                "tests.stub.plugin.impl:FooImpl",
                TypeError,
                "invalid instance type",
            ),
            pytest.param(
                "tests.stub.plugin.impl:BarImpl",
                TypeError,
                ".*missing.*arguments.*",
            ),
        ],
    )
    def test_load_instance_error(
        self,
        bar_loader: Loader[Bar],
        entrypoint: str,
        expected_error: type[Exception],
        expected_message: str,
    ) -> None:
        with pytest.raises(expected_error, match=expected_message):
            bar_loader.load_instance(entrypoint)

    def test_load_constant_ok(self, foo_int_loader: Loader[Foo[int]], foo_int: Foo[int]) -> None:
        obj = foo_int_loader.load_constant("tests.stub.plugin.impl:FOO_INT")

        assert obj is foo_int

    @pytest.mark.parametrize(
        ("entrypoint", "expected_error", "expected_message"),
        [
            pytest.param(
                "invalid-entrypoint",
                ValueError,
                "invalid entrypoint",
            ),
            pytest.param(
                "tests.stub.plugin.unknown_module:UnknownClass",
                ValueError,
                "invalid entrypoint module",
            ),
            pytest.param(
                "tests.stub.plugin.impl:UnknownClass",
                ValueError,
                "invalid entrypoint attribute",
            ),
            pytest.param(
                "tests.stub.plugin.impl:FooImpl",
                TypeError,
                "invalid instance type",
            ),
            pytest.param(
                "tests.stub.plugin.impl:SIMPLE_INT",
                TypeError,
                "invalid instance type",
            ),
        ],
    )
    def test_load_constant_error(
        self,
        foo_int_loader: Loader[Foo[int]],
        entrypoint: str,
        expected_error: type[Exception],
        expected_message: str,
    ) -> None:
        with pytest.raises(expected_error, match=expected_message):
            foo_int_loader.load_constant(entrypoint)


@pytest.fixture
def foo_int_loader() -> Loader[Foo[int]]:
    return Loader[Foo[int]]()


@pytest.fixture
def bar_loader() -> Loader[Bar]:
    return Loader[Bar]()


@pytest.fixture
def foo_int() -> Foo[int]:
    from tests.stub.plugin.impl import FOO_INT

    return FOO_INT
