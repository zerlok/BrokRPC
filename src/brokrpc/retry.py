from __future__ import annotations

import abc
import asyncio
import typing as t
from contextlib import asynccontextmanager, nullcontext
from dataclasses import dataclass
from datetime import timedelta
from math import inf


class Attempt[T]:
    __NOT_SET: t.Final[object] = object()

    def __init__(self, n: int, errors: list[Exception]) -> None:
        self.__n = n
        self.__errors = errors
        self.__result = self.__NOT_SET

    @property
    def ok(self) -> bool:
        return self.__result is not self.__NOT_SET

    @property
    def num(self) -> int:
        return self.__n

    @property
    def is_retry(self) -> bool:
        return self.__n > 0

    @property
    def last_error(self) -> Exception | None:
        return self.__errors[-1] if self.__errors else None

    def result(self) -> T:
        if self.__result is self.__NOT_SET:
            raise RuntimeError

        # NOTE: if result is not __NOT_SET, then it is of type T
        return t.cast(T, self.__result)

    def set_error(self, err: Exception | None) -> None:
        if err is not None:
            self.__errors.append(err)

    def set_result(self, result: T) -> None:
        if self.__result is not self.__NOT_SET:
            raise RuntimeError

        self.__result = result


class RetryerError(Exception):
    pass


class NoMoreAttemptsError(ExceptionGroup, RetryerError):
    pass


class AttemptsTimeoutError(ExceptionGroup, RetryerError):
    pass


class DelayRetryStrategy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def calc_delay(self, i: int) -> timedelta:
        raise NotImplementedError


class ConstantDelay(DelayRetryStrategy):
    def __init__(self, delay: timedelta) -> None:
        self.__delay = delay

    def calc_delay(self, _: int) -> timedelta:
        return self.__delay


class MultiplierDelay(DelayRetryStrategy):
    def __init__(
        self,
        delay: timedelta,
        max_delay: timedelta | None = None,
    ) -> None:
        self.__delay = delay.total_seconds()
        self.__max_delay = max_delay.total_seconds() if max_delay is not None else inf

    def calc_delay(self, i: int) -> timedelta:
        return timedelta(seconds=min(i * self.__delay, self.__max_delay))


class ExponentialDelay(DelayRetryStrategy):
    def __init__(
        self,
        delay: timedelta,
        base: float | None = None,
        max_delay: timedelta | None = None,
    ) -> None:
        self.__delay = delay.total_seconds()
        self.__base = base if base is not None else 2.0
        self.__max_delay = max_delay.total_seconds() if max_delay is not None else inf

    def calc_delay(self, i: int) -> timedelta:
        return timedelta(seconds=min(self.__delay * self.__base ** (i - 1), self.__max_delay))


class RetryStrategy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def is_attempt_limit_reached[V](self, attempt: Attempt[V]) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    async def try_attempt[**U, V](
        self,
        attempt: Attempt[V],
        func: t.Callable[U, t.Awaitable[V]],
        args: U.args,
        kwargs: U.kwargs,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def on_attempt_error[**U, V](
        self,
        func: t.Callable[U, t.Awaitable[V]],
        args: U.args,
        kwargs: U.kwargs,
        error: Exception,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def on_attempt_limit_reached[**U, V](
        self,
        func: t.Callable[U, t.Awaitable[V]],
        args: U.args,
        kwargs: U.kwargs,
        errors: t.Sequence[Exception],
    ) -> Exception | None:
        raise NotImplementedError

    @abc.abstractmethod
    def on_timeout_reached[**U, V](
        self,
        func: t.Callable[U, t.Awaitable[V]],
        args: U.args,
        kwargs: U.kwargs,
        errors: t.Sequence[Exception],
    ) -> Exception | None:
        raise NotImplementedError


class DelayingRetryStrategy(RetryStrategy):
    def __init__(
        self,
        *,
        delay: DelayRetryStrategy,
        errors: t.Sequence[type[Exception]],
        on_attempt_error: t.Callable[[Exception], None] | None = None,
        attempt_limit: int | None = None,
    ) -> None:
        self.__delay = delay
        self.__errors = tuple(errors)
        self.__on_attempt_error = on_attempt_error
        self.__attempt_limit = attempt_limit

    def is_attempt_limit_reached[V](self, attempt: Attempt[V]) -> bool:
        return self.__attempt_limit is not None and attempt.num > self.__attempt_limit

    async def try_attempt[**U, V](
        self,
        attempt: Attempt[V],
        func: t.Callable[U, t.Awaitable[V]],
        args: U.args,
        kwargs: U.kwargs,
    ) -> None:
        if attempt.is_retry and (delay := self.__delay.calc_delay(attempt.num).total_seconds()) > 0:
            await asyncio.sleep(delay)

        try:
            result = await func(*args, **kwargs)

        except self.__errors as err:
            attempt.set_error(err)

        else:
            attempt.set_result(result)

    def on_attempt_error[**U, V](
        self,
        _func: t.Callable[U, t.Awaitable[V]],
        _args: U.args,
        _kwargs: U.kwargs,
        error: Exception,
    ) -> None:
        if self.__on_attempt_error is not None:
            self.__on_attempt_error(error)

    def on_attempt_limit_reached[**U, V](
        self,
        _func: t.Callable[U, t.Awaitable[V]],
        _args: U.args,
        _kwargs: U.kwargs,
        _errors: t.Sequence[Exception],
    ) -> Exception | None:
        return None

    def on_timeout_reached[**U, V](
        self,
        _func: t.Callable[U, t.Awaitable[V]],
        _args: U.args,
        _kwargs: U.kwargs,
        _errors: t.Sequence[Exception],
    ) -> Exception | None:
        return None


class NoRetryStrategy(RetryStrategy):
    def is_attempt_limit_reached[V](self, attempt: Attempt[V]) -> bool:
        return attempt.is_retry

    async def try_attempt[**U, V](
        self,
        attempt: Attempt[V],
        func: t.Callable[U, t.Awaitable[V]],
        args: U.args,
        kwargs: U.kwargs,
    ) -> None:
        attempt.set_result(await func(*args, **kwargs))

    def on_attempt_error[**U, V](
        self,
        func: t.Callable[U, t.Awaitable[V]],
        args: U.args,
        kwargs: U.kwargs,
        error: Exception,
    ) -> None:
        pass

    def on_attempt_limit_reached[**U, V](
        self,
        _func: t.Callable[U, t.Awaitable[V]],
        _args: U.args,
        _kwargs: U.kwargs,
        _errors: t.Sequence[Exception],
    ) -> Exception | None:
        return None

    def on_timeout_reached[**U, V](
        self,
        _func: t.Callable[U, t.Awaitable[V]],
        _args: U.args,
        _kwargs: U.kwargs,
        _errors: t.Sequence[Exception],
    ) -> Exception | None:
        return None


class Retryer:
    def __init__(self, strategy: t.Callable[[], t.AsyncContextManager[RetryStrategy]]) -> None:
        self.__strategy = strategy

    async def do[**U, V](self, func: t.Callable[U, t.Awaitable[V]], /, *args: U.args, **kwargs: U.kwargs) -> V:
        strategy: RetryStrategy | None = None
        errors: list[Exception] = []
        attempt: Attempt[V] = Attempt(0, errors)

        try:
            async with self.__strategy() as strategy:
                while True:
                    if strategy.is_attempt_limit_reached(attempt):
                        if (
                            attempt_limit_err := strategy.on_attempt_limit_reached(func, args, kwargs, errors)
                        ) is not None:
                            raise attempt_limit_err

                        details = "attempts limit was reached"
                        raise NoMoreAttemptsError(details, errors)

                    await strategy.try_attempt(attempt, func, args, kwargs)

                    if attempt.ok:
                        break

                    if (error := attempt.last_error) is not None:
                        strategy.on_attempt_error(func, args, kwargs, error)

                    attempt = Attempt(attempt.num + 1, errors)

        except TimeoutError as err:
            if strategy is None:
                raise

            if (timeout_err := strategy.on_timeout_reached(func, args, kwargs, errors)) is not None:
                raise timeout_err from err

            details = "attempts timeout was reached"
            raise AttemptsTimeoutError(details, errors) from err

        return attempt.result()


@dataclass(frozen=True, kw_only=True)
class DelayRetryOptions:
    retry_delay: timedelta | DelayRetryStrategy | None = None
    retries_limit: int | None = None
    retries_timeout: timedelta | None = None


def create_retryer(
    strategy: RetryStrategy
    | t.AsyncContextManager[RetryStrategy]
    | t.Callable[[], t.AsyncContextManager[RetryStrategy]]
    | None,
) -> Retryer:
    if isinstance(strategy, RetryStrategy):

        def provide() -> t.AsyncContextManager[RetryStrategy]:
            return nullcontext(strategy)

        return Retryer(provide)

    elif isinstance(strategy, t.AsyncContextManager):

        def provide() -> t.AsyncContextManager[RetryStrategy]:
            return strategy

        return Retryer(provide)

    elif callable(strategy):
        return Retryer(strategy)

    elif strategy is None:

        def provide() -> t.AsyncContextManager[RetryStrategy]:
            return nullcontext(NoRetryStrategy())

        return Retryer(provide)

    else:
        t.assert_never(strategy)


def create_delay_retryer(
    options: DelayRetryOptions | None = None,
    errors: t.Sequence[type[Exception]] | None = None,
    on_attempt_error: t.Callable[[Exception], None] | None = None,
) -> Retryer:
    if errors is None:
        return create_retryer(None)

    if options is None or (
        options.retry_delay is None and options.retries_timeout is None and options.retries_limit is None
    ):
        return create_retryer(None)

    timeout = options.retries_timeout.total_seconds() if options.retries_timeout is not None else None

    @asynccontextmanager
    async def provide() -> t.AsyncIterator[RetryStrategy]:
        strategy = DelayingRetryStrategy(
            delay=options.retry_delay
            if isinstance(options.retry_delay, DelayRetryStrategy)
            else ConstantDelay(options.retry_delay)
            if isinstance(options.retry_delay, timedelta)
            else ConstantDelay(timedelta(seconds=0)),
            errors=errors,
            on_attempt_error=on_attempt_error,
            attempt_limit=options.retries_limit,
        )

        async with asyncio.timeout(timeout):
            yield strategy

    return create_retryer(provide)
