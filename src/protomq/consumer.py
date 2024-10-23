from datetime import timedelta

from protomq.abc import Consumer, Publisher, Serializer, UnaryStreamFunc, UnaryUnaryFunc
from protomq.message import ConsumerAck, ConsumerReject, ConsumerResult, ConsumerRetry, Message
from protomq.options import ConsumerOptions, MessageOptions
from protomq.storage import WaiterStorage


class ConsumerExceptionDecorator(Consumer):
    @classmethod
    def from_consume_options(cls, inner: Consumer, options: ConsumerOptions) -> Consumer:
        errors: tuple[type[Exception], ...]

        match options.retry_on_error:
            case True:
                errors = (Exception,)
            case False | None:
                errors = ()
            case tuple():
                errors = options.retry_on_error
            case error_type:
                errors = (error_type,)

        delay: timedelta | None

        match options.retry_delay:
            case float():
                delay = timedelta(seconds=options.retry_delay)
            case value:
                delay = value

        return cls(inner, errors, delay) if errors else inner

    def __init__(
        self,
        inner: Consumer,
        retry_on_exceptions: tuple[type[Exception], ...],
        delay: timedelta | None = None,
    ) -> None:
        self.__inner = inner
        self.__retry_on_exceptions = retry_on_exceptions
        self.__delay = delay

    async def consume(self, message: Message) -> ConsumerResult:
        try:
            result = await self.__inner.consume(message)

        except self.__retry_on_exceptions as err:
            result = ConsumerRetry(
                reason=f"exception occurred: {err}",
                delay=self.__delay,
            )

        return result


class UnaryUnaryConsumer[U, V](Consumer):
    def __init__(
        self,
        inner: UnaryUnaryFunc[U, V],
        serializer: Serializer[U, V],
        responder: Publisher,
    ) -> None:
        self.__inner = inner
        self.__serializer = serializer
        self.__responder = responder

    async def consume(self, request: Message) -> ConsumerResult:
        if request.reply_to is None or request.correlation_id is None:
            return ConsumerReject(reason="reply_to and correlation_id are required")

        request_payload = self.__serializer.load_request(request)
        response_payload: V | Exception
        try:
            response_payload = await self.__inner(request_payload)

        # NOTE: Caught exception will be serialized to response and client will receive it.
        except Exception as err:  # noqa: BLE001
            response_payload = err

        response = self.__serializer.dump_response(
            request=request,
            response=response_payload,
            options=MessageOptions(
                exchange=request.exchange,
                routing_key=request.reply_to,
                correlation_id=request.correlation_id,
            ),
        )
        await self.__responder.publish(response)

        return ConsumerAck()


class UnaryStreamConsumer[U, V](Consumer):
    def __init__(
        self,
        inner: UnaryStreamFunc[U, V],
        serializer: Serializer[U, V],
        responder: Publisher,
    ) -> None:
        self.__inner = inner
        self.__serializer = serializer
        self.__responder = responder

    async def consume(self, request: Message) -> ConsumerResult:
        if request.reply_to is None or request.correlation_id is None:
            return ConsumerReject(reason="reply_to and correlation_id are required")

        request_payload = self.__serializer.load_request(request)

        try:
            async for response_payload in self.__inner(request_payload):
                response = self.__serializer.dump_response(
                    request=request,
                    response=response_payload,
                    options=MessageOptions(
                        exchange=request.exchange,
                        routing_key=request.reply_to,
                        correlation_id=request.correlation_id,
                    ),
                )
                await self.__responder.publish(response)

        # NOTE: Caught exception will be serialized to response and client will receive it.
        except Exception as err:  # noqa: BLE001
            await self.__responder.publish(
                self.__serializer.dump_response(
                    request=request,
                    response=err,
                    options=MessageOptions(
                        exchange=request.exchange,
                        routing_key=request.reply_to,
                        correlation_id=request.correlation_id,
                    ),
                )
            )

        return ConsumerAck()


class UnaryResponseConsumer[U, V](Consumer):
    def __init__(
        self,
        serializer: Serializer[U, V],
        waiters: WaiterStorage[V],
    ) -> None:
        self.__serializer = serializer
        self.__waiters = waiters

    async def consume(self, response: Message) -> ConsumerResult:
        response_payload = self.__serializer.load_response(response)
        if response.correlation_id is None:
            return ConsumerReject()

        if isinstance(response_payload, Exception):
            self.__waiters.set_exception(response.correlation_id, response_payload)

        else:
            self.__waiters.set_result(response.correlation_id, response_payload)

        return ConsumerAck()


# class StreamResponseHandler[T](Consumer):
#     def __init__(
#         self,
#         serializer: Serializer[object, T],
#         waiters: StreamStorage[T],
#     ) -> None:
#         self.__serializer = serializer
#         self.__waiters = waiters
#
#     async def handle(self, response: IncomingMessage) -> None:
#         response_payload = self.__serializer.load_response(response)
#         if response.correlation_id is None:
#             return
#
#         self.__waiters.push_result(response.correlation_id, response_payload)
