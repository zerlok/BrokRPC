import abc
import typing as t

from protomq.model import ConsumerResult, PublisherResult, RawMessage
from protomq.options import BindingOptions, PublisherOptions


class Publisher[U, V](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def publish(self, message: U) -> V:
        raise NotImplementedError


class PublisherMiddleware[S, U, V](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def publish(self, inner: S, message: U) -> V:
        raise NotImplementedError


class Consumer[U, V](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def consume(self, message: U) -> V:
        raise NotImplementedError


class ConsumerMiddleware[S, U, V](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def consume(self, inner: S, message: U) -> V:
        raise NotImplementedError


class BoundConsumer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def is_alive(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def get_options(self) -> BindingOptions:
        raise NotImplementedError


class Serializer[A, B](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def dump_message(self, message: A) -> B:
        raise NotImplementedError

    @abc.abstractmethod
    def load_message(self, message: B) -> A:
        raise NotImplementedError


type RawPublisher = Publisher[RawMessage, PublisherResult]
type RawConsumer = Consumer[RawMessage, ConsumerResult]


class Driver(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def provide_publisher(self, options: PublisherOptions | None = None) -> t.AsyncContextManager[RawPublisher]:
        raise NotImplementedError

    @abc.abstractmethod
    def bind_consumer(self, consumer: RawConsumer, options: BindingOptions) -> t.AsyncContextManager[BoundConsumer]:
        raise NotImplementedError
