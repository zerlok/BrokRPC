import re
import typing as t
from argparse import ArgumentError
from datetime import timedelta

import pytest
from yarl import URL

from brokrpc.cli import Options, Parser


class TestParser:
    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            pytest.param(
                ["amqp://guest:guest@localhost:5672/consume", "consume", "test-rk-consume"],
                Options(
                    url=URL("amqp://guest:guest@localhost:5672/consume"),
                    mode="consume",
                    routing_key="test-rk-consume",
                ),
                id="simple consumer",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/publish", "publish", "test-rk-pub"],
                Options(
                    url=URL("amqp://guest:guest@localhost:5672/publish"),
                    mode="publish",
                    routing_key="test-rk-pub",
                ),
                id="simple publisher",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/publish", "check"],
                Options(
                    url=URL("amqp://guest:guest@localhost:5672/publish"),
                    mode="check",
                ),
                id="simple checker",
            ),
            pytest.param(
                [
                    "amqp://guest:guest@localhost:5672/publish",
                    "--retry-delay=3",
                    "--retry-delay-mode=exponential",
                    "--retries-limit=4",
                    "check",
                ],
                Options(
                    url=URL("amqp://guest:guest@localhost:5672/publish"),
                    mode="check",
                    retry_delay=timedelta(seconds=3),
                    retry_delay_mode="exponential",
                    retries_limit=4,
                ),
                id="checker with exponential retryer",
            ),
        ],
    )
    def test_parse_ok(self, parser: Parser, args: t.Sequence[str], expected: Options) -> None:
        assert parser.parse(args, Options()) == expected

    @pytest.mark.parametrize(
        ("args", "expected_message"),
        [
            pytest.param(
                [],
                "the following arguments are required: url",
                id="url is required",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/"],
                "the following arguments are required: {publish,consume,check}",
                id="mode is required",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/", "consume", "test-rk", "--unknown-param"],
                "unrecognized arguments: --unknown-param",
                id="unknown param",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/", "consume", "test-rk", "--decoder=invalid"],
                "argument --decoder: invalid __parse_decoder value: 'invalid'",
                id="invalid serializer",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/", "--retry-delay-mode=unknown", "checker"],
                re.escape(
                    "argument --retry-delay-mode: invalid choice: 'unknown' (choose from 'constant', 'multiplier', "
                    "'exponential')"
                ),
                id="invalid retry delay mode",
            ),
        ],
    )
    def test_parse_error(self, parser: Parser, args: t.Sequence[str], expected_message: str) -> None:
        with pytest.raises(ArgumentError, match=expected_message):
            parser.parse(args)


@pytest.fixture
def parser() -> Parser:
    return Parser(exit_on_error=False)
