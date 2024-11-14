import abc
import typing as t

from brokrpc.rpc.model import BinaryRequest, BinaryResponse, Request, Response

type UnaryUnaryFunc[U, V] = t.Callable[[U], t.Awaitable[V]]
type UnaryStreamFunc[U, V] = t.Callable[[U], t.AsyncIterable[V]]
type StreamUnaryFunc[U, V] = t.Callable[[t.AsyncIterator[U]], t.Awaitable[V]]
type StreamStreamFunc[U, V] = t.Callable[[t.AsyncIterator[U]], t.AsyncIterable[V]]
type HandlerFunc[U, V] = UnaryUnaryFunc[U, V] | UnaryStreamFunc[U, V] | StreamUnaryFunc[U, V] | StreamStreamFunc[U, V]


class Caller[U, V](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def invoke(self, request: U) -> Response[V]:
        raise NotImplementedError


class HandlerSerializer[U, V](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def load_unary_request(self, request: BinaryRequest) -> Request[U]:
        raise NotImplementedError

    @abc.abstractmethod
    def dump_unary_response(self, response: Response[V]) -> BinaryResponse:
        raise NotImplementedError


class CallerSerializer[U, V](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def dump_unary_request(self, request: Request[U]) -> BinaryRequest:
        raise NotImplementedError

    @abc.abstractmethod
    def load_unary_response(self, response: BinaryResponse) -> Response[V]:
        raise NotImplementedError


class RPCSerializer[U, V](HandlerSerializer[U, V], CallerSerializer[U, V], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def dump_unary_request(self, request: Request[U]) -> BinaryRequest:
        raise NotImplementedError

    @abc.abstractmethod
    def load_unary_request(self, request: BinaryRequest) -> Request[U]:
        raise NotImplementedError

    @abc.abstractmethod
    def dump_unary_response(self, response: Response[V]) -> BinaryResponse:
        raise NotImplementedError

    @abc.abstractmethod
    def load_unary_response(self, response: BinaryResponse) -> Response[V]:
        raise NotImplementedError

    # @abc.abstractmethod
    # def dump_stream_request(
    #     self,
    #     request: t.Sequence[U],
    #     options: MessageOptions,
    # ) -> t.Sequence[StreamRequest]:
    #     raise NotImplementedError
    #
    # @abc.abstractmethod
    # def load_stream_request(self, request: t.Sequence[StreamRequest]) -> t.Sequence[U]:
    #     raise NotImplementedError
    #
    # @abc.abstractmethod
    # def dump_stream_response(
    #     self,
    #     request: StreamRequest,
    #     response: t.Sequence[V | Exception],
    #     options: MessageOptions,
    # ) -> t.Sequence[StreamResponse]:
    #     raise NotImplementedError
    #
    # @abc.abstractmethod
    # def load_stream_response(self, response: t.Sequence[StreamResponse]) -> t.Sequence[V | Exception]:
    #     raise NotImplementedError
