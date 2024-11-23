from tests.stub.plugin.abc import Bar, Foo


class FooImpl[T](Foo[T]):
    def do_foo_stuff(self, value: T) -> str:
        return str(value)


class BarImpl(Bar):
    def __init__(self, start: int, end: int) -> None:
        self.__start = start
        self.__end = end

    def do_bar_stuff(self, value: str) -> str:
        return value[self.__start : self.__end : -1]


FOO_INT = FooImpl[int]()

SIMPLE_INT = 42


def load_bar() -> Bar:
    return BarImpl(0, 100)
