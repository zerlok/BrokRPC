from protomq.model import Message, RawMessage
from protomq.rpc.abc import CallerSerializer, HandlerSerializer
from protomq.rpc.model import Request, Response


class RawRPCSerializer(CallerSerializer[bytes, Message[bytes]], HandlerSerializer[Request[bytes], bytes]):
    def dump_unary_request(self, request: Request[bytes]) -> Request[bytes]:
        return request

    def load_unary_request(self, request: Request[bytes]) -> Request[bytes]:
        return request

    def dump_unary_response(self, response: Response[Request[bytes], bytes]) -> RawMessage:
        return response

    def load_unary_response(self, response: RawMessage) -> Message[bytes]:
        return response
