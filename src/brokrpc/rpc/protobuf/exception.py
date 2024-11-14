from brokrpc.message import Message


class CallError(Exception):
    def __init__(
        self,
        description: str | None = None,
        response: Message[object] | None = None,
    ) -> None:
        self.description = description
        self.response = response

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}: {self.description or ""}; response={self.response}>"

    @property
    def correlation_id(self) -> str | None:
        return self.response.correlation_id if self.response is not None else None
