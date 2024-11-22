import abc


class Foo[T](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def do_foo_stuff(self, value: T) -> str:
        raise NotImplementedError


class Bar(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def do_bar_stuff(self, value: str) -> str:
        raise NotImplementedError
