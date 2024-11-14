from __future__ import annotations

import asyncio
import typing as t
from dataclasses import dataclass
from datetime import timedelta
from itertools import count


@dataclass(frozen=True, kw_only=True)
class RetryOptions:
    retry_delay_mode: t.Literal["constant", "multiplier", "exponential"] | None = None
    retry_delay: timedelta | None = None
    retry_max_delay: timedelta | None = None
    retries_timeout: timedelta | None = None
    retries_limit: int | None = None

    def __post_init__(self) -> None:
        if self.retry_delay is not None and self.retry_delay.total_seconds() < 0.0:
            details = "retry delay must be greater than or equal to 0"
            raise ValueError(details, self)

        if self.retry_max_delay is not None and self.retry_max_delay.total_seconds() < 0.0:
            details = "retry max delay must be greater than or equal to 0"
            raise ValueError(details, self)

        if self.retries_timeout is not None and self.retries_timeout.total_seconds() <= 0.0:
            details = "retries timeout must be greater than 0"
            raise ValueError(details, self)

        if self.retries_limit is not None and self.retries_limit < 0:
            details = "retries limit must be greater than or equal to 0"
            raise ValueError(details, self)

        if (
            self.retry_max_delay is not None
            and self.retry_delay is not None
            and self.retry_delay > self.retry_max_delay
        ):
            details = "retry delay must be less than or equal to retry max delay"
            raise ValueError(details, self)


class Attempt:
    def __init__(self, n: int, errors: t.List[Exception]) -> None:
        self.__n = n
        self.__errors = errors
        self.__ok: bool | None = None

    @property
    def ok(self) -> bool | None:
        return self.__ok

    @property
    def num(self) -> int:
        return self.__n

    @property
    def is_retry(self) -> bool:
        return self.__n > 0

    def set_failed(self, err: Exception | None) -> None:
        if self.__ok is not None:
            return

        self.__ok = False
        if err is not None:
            self.__errors.append(err)

    def set_succeeded(self) -> None:
        if self.__ok is not None:
            return

        self.__ok = True


class RetryerError(Exception):
    pass


class NoMoreAttemptsError(ExceptionGroup, RetryerError):
    pass


class BoundRetryer:
    def __init__(
        self,
        retryer: Retryer,
        retry_on_exceptions: tuple[type[Exception], ...],
        no_more_attempts_message: str,
    ) -> None:
        self.__retryer = retryer
        self.__retry_on_exceptions = retry_on_exceptions
        self.__no_more_attempts_message = no_more_attempts_message

    async def do[**U, V](self, func: t.Callable[U, t.Awaitable[V]], *args: U.args, **kwargs: U.kwargs) -> V:
        async for attempt in self.__retryer.iterate(self.__no_more_attempts_message):
            try:
                result = await func(*args, **kwargs)

            except self.__retry_on_exceptions as err:
                attempt.set_failed(err)

            else:
                attempt.set_succeeded()
                return result

        # NOTE: it's always at least one attempt.
        raise RuntimeError


class Retryer:
    def __init__(self, options: RetryOptions) -> None:
        self.__options = options

    async def iterate(self, no_more_attempts_message: str) -> t.AsyncIterator[Attempt]:
        errors: list[Exception] = []

        timeout = self.__options.retries_timeout.total_seconds() if self.__options.retries_timeout is not None else None
        try:
            async with asyncio.timeout(timeout):
                async for attempt in self.__gen_attempts(errors):
                    yield attempt

                    if attempt.ok:
                        return

        except asyncio.TimeoutError as err:
            errors.append(err)

        raise NoMoreAttemptsError(no_more_attempts_message, errors)

    def bind(
        self,
        retry_on_exceptions: tuple[type[Exception], ...],
        no_more_attempts_message: str,
    ) -> BoundRetryer:
        return BoundRetryer(self, retry_on_exceptions, no_more_attempts_message)

    async def __gen_attempts(self, errors: t.List[Exception]) -> t.AsyncIterable[Attempt]:
        retries_limit = self.__options.retries_limit or 0

        attempt_delay_factory = self.__provide_attempt_delay_factory()

        # i == 0 is a first attempt, it's not a retry yet
        for i in count():
            if i > retries_limit:
                return

            delay = attempt_delay_factory(i)
            if delay:
                await asyncio.sleep(delay)

            yield Attempt(i, errors)

    def __provide_attempt_delay_factory(self) -> t.Callable[[int], float | None]:
        if self.__options.retry_delay is None:

            def get_no_delay(_: int) -> None:
                return None

            return get_no_delay

        mode = self.__options.retry_delay_mode if self.__options.retry_delay_mode is not None else "multiplier"
        base_delay = self.__options.retry_delay.total_seconds()

        if self.__options.retry_max_delay is not None:
            max_delay = self.__options.retry_max_delay.total_seconds()

            def clamp_delay(value: float) -> float:
                return min(value, max_delay)

        else:

            def clamp_delay(value: float) -> float:
                return value

        if mode == "constant":

            def get_const_delay(_: int) -> float:
                return base_delay

            return get_const_delay

        elif mode == "multiplier":

            def get_mul_delay(i: int) -> float:
                return clamp_delay(base_delay * i)

            return get_mul_delay

        elif mode == "exponential":

            def get_exp_delay(i: int) -> float:
                return clamp_delay(base_delay**i)

            return get_exp_delay

        else:
            t.assert_never(mode)
