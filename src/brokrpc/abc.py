import abc
import typing as t

from brokrpc.message import BinaryMessage
from brokrpc.model import ConsumerResult, PublisherResult
from brokrpc.options import BindingOptions, PublisherOptions


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


class MessageEncoder[A, B](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def encode_message(self, message: A) -> B:
        raise NotImplementedError


class MessageDecoder[A, B](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def decode_message(self, message: B) -> A:
        raise NotImplementedError


class Serializer[A, B](MessageEncoder[A, B], MessageDecoder[A, B], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def encode_message(self, message: A) -> B:
        raise NotImplementedError

    @abc.abstractmethod
    def decode_message(self, message: B) -> A:
        raise NotImplementedError


type BinaryPublisher = Publisher[BinaryMessage, PublisherResult]
type BinaryConsumer = Consumer[BinaryMessage, ConsumerResult]


class BrokerDriver(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def provide_publisher(self, options: PublisherOptions | None = None) -> t.AsyncContextManager[BinaryPublisher]:
        raise NotImplementedError

    @abc.abstractmethod
    def bind_consumer(self, consumer: BinaryConsumer, options: BindingOptions) -> t.AsyncContextManager[BoundConsumer]:
        raise NotImplementedError
