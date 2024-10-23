import abc
import typing as t

from protomq.message import ConsumerResult, Message
from protomq.options import BindingOptions, MessageOptions, PublisherOptions


class UnaryUnaryFunc[U, V](t.Protocol):
    @abc.abstractmethod
    async def __call__(self, request: U) -> V:
        raise NotImplementedError


class UnaryStreamFunc[U, V](t.Protocol):
    @abc.abstractmethod
    def __call__(self, request: U) -> t.AsyncIterable[V]:
        raise NotImplementedError


class StreamUnaryFunc[U, V](t.Protocol):
    @abc.abstractmethod
    async def __call__(self, request: t.AsyncIterable[U]) -> V:
        raise NotImplementedError


class StreamStreamFunc[U, V](t.Protocol):
    @abc.abstractmethod
    def __call__(self, request: t.AsyncIterable[U]) -> t.AsyncIterable[V]:
        raise NotImplementedError


class Serializer[U, V](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def load_request(self, request: Message) -> U:
        raise NotImplementedError

    @abc.abstractmethod
    def dump_request(self, request: U, options: MessageOptions) -> Message:
        raise NotImplementedError

    @abc.abstractmethod
    def load_response(self, response: Message) -> V | Exception:
        raise NotImplementedError

    @abc.abstractmethod
    def dump_response(self, request: Message, response: V | Exception, options: MessageOptions) -> Message:
        raise NotImplementedError


class Publisher(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def publish(self, message: Message) -> None:
        raise NotImplementedError


class Consumer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def consume(self, message: Message) -> ConsumerResult:
        raise NotImplementedError


class BoundConsumer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def is_alive(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def get_options(self) -> BindingOptions:
        raise NotImplementedError


class Driver(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def provide_publisher(self, options: PublisherOptions | None = None) -> t.AsyncContextManager[Publisher]:
        raise NotImplementedError

    @abc.abstractmethod
    def bind_consumer(self, consumer: Consumer, options: BindingOptions) -> t.AsyncContextManager[BoundConsumer]:
        raise NotImplementedError
