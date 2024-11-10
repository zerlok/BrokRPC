import abc
import typing as t

from protomq.model import RawMessage
from protomq.rpc.model import Request, Response

U_contra = t.TypeVar("U_contra", contravariant=True)
V_co = t.TypeVar("V_co", covariant=True)


type UnaryUnaryFunc[U_contra, V_co] = t.Callable[[U_contra], t.Awaitable[V_co]]
type UnaryStreamFunc[U_contra, V_co] = t.Callable[[U_contra], t.AsyncIterable[V_co]]
type StreamUnaryFunc[U_contra, V_co] = t.Callable[[t.AsyncIterator[U_contra]], t.Awaitable[V_co]]
type StreamStreamFunc[U_contra, V_co] = t.Callable[[t.AsyncIterator[U_contra]], t.AsyncIterable[V_co]]

type HandlerFunc[U_contra, V_co] = (
    UnaryUnaryFunc[U_contra, V_co]
    | UnaryStreamFunc[U_contra, V_co]
    | StreamUnaryFunc[U_contra, V_co]
    | StreamStreamFunc[U_contra, V_co]
)


class Caller(t.Generic[U_contra, V_co], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def invoke(self, request: U_contra) -> V_co:
        raise NotImplementedError


class HandlerSerializer(t.Generic[U_contra, V_co], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def load_unary_request(self, request: Request[bytes]) -> U_contra:
        raise NotImplementedError

    @abc.abstractmethod
    def dump_unary_response(self, response: Response[U_contra, V_co]) -> RawMessage:
        raise NotImplementedError


class CallerSerializer(t.Generic[U_contra, V_co], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def dump_unary_request(self, request: Request[U_contra]) -> Request[bytes]:
        raise NotImplementedError

    @abc.abstractmethod
    def load_unary_response(self, response: RawMessage) -> V_co:
        raise NotImplementedError


class RPCSerializer[U, V](HandlerSerializer[U, V], CallerSerializer[U, V], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def dump_unary_request(self, request: Request[U]) -> Request[bytes]:
        raise NotImplementedError

    @abc.abstractmethod
    def load_unary_request(self, request: Request[bytes]) -> U:
        raise NotImplementedError

    @abc.abstractmethod
    def dump_unary_response(self, response: Response[U, V]) -> RawMessage:
        raise NotImplementedError

    @abc.abstractmethod
    def load_unary_response(self, response: RawMessage) -> V:
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
