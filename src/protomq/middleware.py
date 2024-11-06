from __future__ import annotations

from datetime import timedelta

from protomq.abc import Consumer, ConsumerMiddleware, Publisher, PublisherMiddleware
from protomq.message import ConsumerResult, ConsumerRetry
from protomq.options import ConsumerOptions


class PublisherMiddlewareWrapper[T, U, V](Publisher[U, V]):
    def __init__(self, inner: T, middleware: PublisherMiddleware[T, U, V]) -> None:
        self.__inner = inner
        self.__middleware = middleware

    async def publish(self, message: U) -> V:
        return await self.__middleware.publish(self.__inner, message)


class ConsumerMiddlewareWrapper[T, U, V](Consumer[U, V]):
    def __init__(self, inner: T, middleware: ConsumerMiddleware[T, U, V]) -> None:
        self.__inner = inner
        self.__middleware = middleware

    async def consume(self, message: U) -> V:
        return await self.__middleware.consume(self.__inner, message)


class RetryOnErrorConsumerMiddleware[T](ConsumerMiddleware[Consumer[T, ConsumerResult], T, ConsumerResult]):
    @classmethod
    def from_consume_options(cls, options: ConsumerOptions) -> RetryOnErrorConsumerMiddleware[T]:
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

        return cls(errors, delay)

    def __init__(
        self,
        retry_on_exceptions: tuple[type[Exception], ...],
        delay: timedelta | None = None,
    ) -> None:
        self.__retry_on_exceptions = retry_on_exceptions
        self.__delay = delay

    async def consume(self, inner: Consumer[T, ConsumerResult], message: T) -> ConsumerResult:
        try:
            result = await inner.consume(message)

        except self.__retry_on_exceptions as err:
            result = ConsumerRetry(
                reason=f"{inner} exception occurred: {err}",
                delay=self.__delay,
            )

        return result
