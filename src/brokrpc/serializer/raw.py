from brokrpc.rpc.abc import CallerSerializer, HandlerSerializer
from brokrpc.rpc.model import BinaryRequest, BinaryResponse


class RawRPCSerializer(CallerSerializer[bytes, bytes], HandlerSerializer[bytes, bytes]):
    def dump_unary_request(self, request: BinaryRequest) -> BinaryRequest:
        return request

    def load_unary_request(self, request: BinaryRequest) -> BinaryRequest:
        return request

    def dump_unary_response(self, response: BinaryResponse) -> BinaryResponse:
        return response

    def load_unary_response(self, response: BinaryResponse) -> BinaryResponse:
        return response
