from __future__ import annotations

import logging
import typing as t
from datetime import timedelta

from brokrpc.abc import Consumer, ConsumerMiddleware, Publisher, PublisherMiddleware
from brokrpc.model import (
    ConsumerReject,
    ConsumerResult,
    ConsumerRetry,
    PublisherResult,
    SerializerDumpError,
    SerializerLoadError,
)
from brokrpc.retry import ConstantDelay, DelayRetryStrategy
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
    def __init__(
        self,
        errors: t.Sequence[type[Exception]],
        delay: timedelta | DelayRetryStrategy | None = None,
    ) -> None:
        self.__delay = (
            delay
            if isinstance(delay, DelayRetryStrategy)
            else ConstantDelay(delay)
            if isinstance(delay, timedelta)
            else None
        )
        self.__errors = tuple(errors)

    async def consume(self, inner: Consumer[T, ConsumerResult], message: T) -> ConsumerResult:
        try:
            result = await inner.consume(message)

        except self.__errors as err:
            logging.warning("%s consumer failed on %s, retrying", inner, message, exc_info=err)
            result = ConsumerRetry(
                reason=f"{inner} exception occurred: {err}",
                # TODO: get message consumption attempt count
                delay=self.__delay.calc_delay(1) if self.__delay is not None else None,
            )

        return result
