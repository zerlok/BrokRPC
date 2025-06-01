import typing as t
from argparse import ArgumentParser

import pytest
from yarl import URL

from brokrpc.cli import CLIOptions, build_parser
from brokrpc.serializer.ident import IdentSerializer
from brokrpc.serializer.json import JSONSerializer
from tests.check import check_instance


class TestCLIOptions:
    @pytest.mark.parametrize(
        "values",
        [
            pytest.param(
                {
                    "url": URL("amqp://guest:guest@localhost:5672/consume"),
                    "routing_key": "test-rk-consume",
                    "kind": "consumer",
                    "exchange": None,
                    "output_mode": "body",
                    "quiet": False,
                    "serializer": JSONSerializer(),
                }
            ),
        ],
    )
    def test_to_str(self, options: CLIOptions, values: t.Mapping[str, object] | None) -> None:
        cli_options_str = str(options)
        assert all(key in cli_options_str and str(value) in cli_options_str for key, value in (values or {}).items())


class TestParser:
    @pytest.mark.parametrize(
        ("args", "expected_ns"),
        [
            pytest.param(
                ["amqp://guest:guest@localhost:5672/consume", "test-rk-consume", "--consumer"],
                {
                    "url": URL("amqp://guest:guest@localhost:5672/consume"),
                    "routing_key": "test-rk-consume",
                    "kind": "consumer",
                    "exchange": None,
                    "output_mode": "body",
                    "quiet": False,
                    "serializer": check_instance(IdentSerializer),
                },
                id="simple consumer",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/publish", "test-rk-pub", "--publisher"],
                {
                    "url": URL("amqp://guest:guest@localhost:5672/publish"),
                    "routing_key": "test-rk-pub",
                    "kind": "publisher",
                    "exchange": None,
                    "output_mode": "body",
                    "quiet": False,
                    "serializer": check_instance(IdentSerializer),
                },
                id="simple publisher",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/json-pub", "test-rk-json-pub", "--publisher", "--serializer=json"],
                {
                    "url": URL("amqp://guest:guest@localhost:5672/json-pub"),
                    "routing_key": "test-rk-json-pub",
                    "kind": "publisher",
                    "exchange": None,
                    "output_mode": "body",
                    "quiet": False,
                    "serializer": check_instance(JSONSerializer),
                },
                id="json publisher",
            ),
        ],
    )
    def test_parse_ok(self, parser: ArgumentParser, args: t.Sequence[str], expected_ns: t.Mapping[str, object]) -> None:
        assert parser.parse_args(args, namespace=CLIOptions()).__dict__ == expected_ns

    @pytest.mark.parametrize(
        ("args", "expected_message"),
        [
            pytest.param(
                [],
                "the following arguments are required: url, routing_key",
                id="url & routing key are required",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/", "test-rk"],
                "one of the arguments -c/--consumer -p/--publisher is required",
                id="consumer or publisher is required",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/", "test-rk", "--consumer", "--unknown-param"],
                "unrecognized arguments: --unknown-param",
                id="unknown param",
            ),
            pytest.param(
                ["amqp://guest:guest@localhost:5672/", "test-rk", "--consumer", "--serializer=invalid"],
                "argument -s/--serializer: invalid parse_serializer value: 'invalid'",
                id="invalid serializer",
            ),
        ],
    )
    def test_parse_error(self, parser: ArgumentParser, args: t.Sequence[str], expected_message: str) -> None:
        with pytest.raises(ExitError, match=expected_message):
            parser.parse_args(args)


@pytest.fixture
def values() -> t.Mapping[str, object] | None:
    return None


@pytest.fixture
def options(values: t.Mapping[str, object] | None) -> CLIOptions:
    options = CLIOptions()

    for key, value in (values or {}).items():
        setattr(options, key, value)

    return options


@pytest.fixture
def parser() -> ArgumentParser:
    parser = build_parser()

    # NOTE: patch `error` method: raise `ExitError` instead of `sys.exit`
    def error(message: str) -> t.NoReturn:
        raise ExitError(message)

    parser.error = error  # type: ignore[method-assign]

    return parser


class ExitError(Exception):
    pass
