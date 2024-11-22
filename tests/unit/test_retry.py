from __future__ import annotations

import asyncio
import typing as t
from datetime import UTC, datetime, timedelta

import pytest

from brokrpc.retry import (
    AttemptsTimeoutError,
    ConstantDelay,
    DelayRetryOptions,
    ExponentialDelay,
    MultiplierDelay,
    NoMoreAttemptsError,
    Retryer,
    create_delay_retryer,
)


class CustomFooError(Exception):
    pass


class CustomBarError(Exception):
    pass


class Foo:
    def __init__(self, max_n: int = 0) -> None:
        self.__calls: list[datetime] = []
        self.__max_n = max_n

    @property
    def cntr(self) -> int:
        return len(self.__calls)

    @property
    def call_delays(self) -> t.Sequence[float]:
        return [
            0,
            *(
                (next_at - call_at).total_seconds()
                for call_at, next_at in zip(self.__calls, self.__calls[1:], strict=False)
            ),
        ]

    @property
    def max_n(self) -> int:
        return self.__max_n

    async def __call__(self, obj: object) -> str:
        self.__calls.append(datetime.now(UTC))
        await asyncio.sleep(0)

        if self.cntr <= self.__max_n:
            raise CustomFooError(self.cntr, obj)

        return str(obj)


class TestRetryer:
    @pytest.mark.parametrize(
        (
            "foo",
            "retryer",
            "obj",
        ),
        [
            pytest.param(
                Foo(),
                create_delay_retryer(
                    options=DelayRetryOptions(),
                    errors=[],
                ),
                object(),
                id="without retry",
            ),
            pytest.param(
                Foo(1),
                create_delay_retryer(
                    options=DelayRetryOptions(
                        retries_limit=5,
                    ),
                    errors=[CustomFooError],
                ),
                object(),
                id="1 attempt",
            ),
            pytest.param(
                Foo(5),
                create_delay_retryer(
                    options=DelayRetryOptions(
                        retries_limit=5,
                    ),
                    errors=[CustomFooError],
                ),
                object(),
                id="5 attempts",
            ),
        ],
    )
    async def test_retry_ok(self, retryer: Retryer, foo: Foo, obj: object) -> None:
        result = await retryer.do(foo, obj)

        assert foo.cntr == foo.max_n + 1
        assert result == str(obj)

    @pytest.mark.parametrize(
        (
            "foo",
            "retryer",
            "expected_error",
            "obj",
        ),
        [
            pytest.param(
                Foo(1),
                create_delay_retryer(
                    options=DelayRetryOptions(
                        retries_limit=5,
                    ),
                    errors=[
                        CustomBarError,
                    ],
                ),
                CustomFooError,
                object(),
                id="unhandled custom foo error",
            ),
            pytest.param(
                Foo(6),
                create_delay_retryer(
                    options=DelayRetryOptions(
                        retries_limit=5,
                    ),
                    errors=[CustomFooError],
                ),
                NoMoreAttemptsError,
                object(),
                id="no more attempts",
            ),
        ],
    )
    async def test_retry_error(
        self,
        retryer: Retryer,
        foo: Foo,
        obj: object,
        expected_error: type[Exception],
    ) -> None:
        with pytest.raises(expected_error):
            await retryer.do(foo, obj)

    @pytest.mark.parametrize(
        (
            "foo",
            "retryer",
            "expected_delays",
            "obj",
        ),
        [
            pytest.param(
                Foo(4),
                create_delay_retryer(
                    options=DelayRetryOptions(
                        retry_delay=ConstantDelay(delay=timedelta(milliseconds=20)),
                        retries_limit=5,
                    ),
                    errors=[CustomFooError],
                ),
                [
                    timedelta(seconds=0),
                    timedelta(milliseconds=20),
                    timedelta(milliseconds=20),
                    timedelta(milliseconds=20),
                    timedelta(milliseconds=20),
                ],
                object(),
                id="constant",
            ),
            pytest.param(
                Foo(4),
                create_delay_retryer(
                    options=DelayRetryOptions(
                        retry_delay=MultiplierDelay(delay=timedelta(milliseconds=20)),
                        retries_limit=5,
                    ),
                    errors=[CustomFooError],
                ),
                [
                    timedelta(seconds=0),
                    timedelta(milliseconds=20),
                    timedelta(milliseconds=40),
                    timedelta(milliseconds=60),
                    timedelta(milliseconds=80),
                ],
                object(),
                id="multiplier",
            ),
            pytest.param(
                Foo(4),
                create_delay_retryer(
                    options=DelayRetryOptions(
                        retry_delay=ExponentialDelay(delay=timedelta(milliseconds=20)),
                        retries_limit=5,
                    ),
                    errors=[CustomFooError],
                ),
                [
                    timedelta(seconds=0),
                    timedelta(milliseconds=20),
                    timedelta(milliseconds=40),
                    timedelta(milliseconds=80),
                    timedelta(milliseconds=160),
                ],
                object(),
                id="exponential",
            ),
            pytest.param(
                Foo(4),
                create_delay_retryer(
                    options=DelayRetryOptions(
                        retry_delay=ExponentialDelay(
                            delay=timedelta(milliseconds=20),
                            max_delay=timedelta(milliseconds=50),
                        ),
                        retries_limit=5,
                    ),
                    errors=[CustomFooError],
                ),
                [
                    timedelta(seconds=0),
                    timedelta(milliseconds=20),
                    timedelta(milliseconds=40),
                    timedelta(milliseconds=50),
                    timedelta(milliseconds=50),
                ],
                object(),
                id="exponential with max",
            ),
        ],
    )
    async def test_retry_delay(
        self,
        retryer: Retryer,
        foo: Foo,
        obj: object,
        expected_delays: t.Sequence[timedelta],
    ) -> None:
        await retryer.do(foo, obj)

        # NOTE: `pytest.approx` is a common practice to compare float results in tests.
        assert foo.call_delays == [
            pytest.approx(delay.total_seconds(), abs=timedelta(milliseconds=10).total_seconds())
            for delay in expected_delays
        ]  # type: ignore[comparison-overlap]

    @pytest.mark.parametrize(
        (
            "foo",
            "retryer",
            "obj",
        ),
        [
            pytest.param(
                Foo(100),
                create_delay_retryer(
                    options=DelayRetryOptions(
                        retry_delay=timedelta(milliseconds=20),
                        retries_limit=99,
                        retries_timeout=timedelta(milliseconds=100),
                    ),
                    errors=[CustomFooError],
                ),
                object(),
            ),
        ],
    )
    async def test_retry_timeout(
        self,
        retryer: Retryer,
        foo: Foo,
        obj: object,
    ) -> None:
        with pytest.raises(AttemptsTimeoutError):
            await retryer.do(foo, obj)

        assert foo.cntr < 10
