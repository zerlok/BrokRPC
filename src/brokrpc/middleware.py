from __future__ import annotations

import logging
import typing as t
from datetime import timedelta

if t.TYPE_CHECKING:
    from brokrpc.options import ConsumerOptions

from brokrpc.abc import Consumer, ConsumerMiddleware, Publisher, PublisherMiddleware
from brokrpc.model import (
    ConsumerReject,
    ConsumerResult,
    ConsumerRetry,
    PublisherResult,
    SerializerDumpError,
    SerializerLoadError,
)
from brokrpc.stringify import to_str_obj


class PublisherMiddlewareWrapper[T, U, V](Publisher[U, V]):
    def __init__(self, inner: T, middleware: PublisherMiddleware[T, U, V]) -> None:
        self.__inner = inner
        self.__middleware = middleware

    def __str__(self) -> str:
        return to_str_obj(self, inner=self.__inner, middleware=self.__middleware)

    async def publish(self, message: U) -> V:
        return await self.__middleware.publish(self.__inner, message)


class ConsumerMiddlewareWrapper[T, U, V](Consumer[U, V]):
    def __init__(self, inner: T, middleware: ConsumerMiddleware[T, U, V]) -> None:
        self.__inner = inner
        self.__middleware = middleware

    def __str__(self) -> str:
        return to_str_obj(self, inner=self.__inner, middleware=self.__middleware)

    async def consume(self, message: U) -> V:
        return await self.__middleware.consume(self.__inner, message)


class AbortBadMessageMiddleware[T](
    PublisherMiddleware[Publisher[T, PublisherResult], T, PublisherResult],
    ConsumerMiddleware[Consumer[T, ConsumerResult], T, ConsumerResult],
):
    async def publish(self, inner: Publisher[T, PublisherResult], message: T) -> PublisherResult:
        try:
            result = await inner.publish(message)

        except SerializerDumpError as err:
            logging.warning("%s publisher serializer failed on %s, aborting", inner, message, exc_info=err)
            result = False

        return result

    async def consume(self, inner: Consumer[T, ConsumerResult], message: T) -> ConsumerResult:
        try:
            result = await inner.consume(message)

        except SerializerLoadError as err:
            logging.warning("%s consumer serializer failed on %s, aborting", inner, message, exc_info=err)
            result = ConsumerReject(
                reason=f"{inner} exception occurred: {err}",
            )

        return result


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
            logging.warning("%s consumer failed on %s, retrying", inner, message, exc_info=err)
            result = ConsumerRetry(
                reason=f"{inner} exception occurred: {err}",
                delay=self.__delay,
            )

        return result
