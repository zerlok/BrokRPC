import abc

from brokrpc.rpc.model import BinaryRequest, BinaryResponse, Request, Response


class UnaryUnaryHandler[U, V](metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def handle(self, request: U) -> V:
        raise NotImplementedError


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
