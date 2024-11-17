import asyncio
import sys
import typing as t
from argparse import ArgumentParser
from functools import partial
from uuid import uuid4

from yarl import URL

from brokrpc.abc import Serializer
from brokrpc.broker import Broker
from brokrpc.entrypoint import load_class, load_instance
from brokrpc.message import AppMessage, BinaryMessage, Message
from brokrpc.middleware import AbortBadMessageMiddleware
from brokrpc.options import BindingOptions, ExchangeOptions, PublisherOptions, QueueOptions
from brokrpc.serializer.ident import IdentSerializer


class CLIOptions:
    url: URL
    exchange: ExchangeOptions | None
    routing_key: str
    kind: t.Literal["consumer", "publisher"]
    serializer: Serializer[Message[object], BinaryMessage]
    input: t.IO[bytes]
    output: t.IO[bytes]
    err: t.IO[bytes]


async def run(options: CLIOptions) -> int:
    async with Broker(
        options.url,
        default_publisher_middlewares=[
            AbortBadMessageMiddleware(),
        ],
        default_consumer_middlewares=[
            AbortBadMessageMiddleware(),
        ],
    ) as broker:
        match options.kind:
            case "consumer":
                return await run_consumer(broker, options)

            case "publisher":
                return await run_publisher(broker, options)

            case _:
                t.assert_never(options.kind)


async def run_consumer(broker: Broker, options: CLIOptions) -> int:
    async with broker.consumer(
        partial(consume_message, options.output),
        BindingOptions(
            exchange=options.exchange,
            binding_keys=[options.routing_key],
            queue=QueueOptions(
                name=f"{__name__}.{uuid4().hex}",
                exclusive=True,
                auto_delete=True,
                prefetch_count=1,
            ),
        ),
        serializer=options.serializer,
    ):
        async for _ in read_lines(options.input):
            pass

        return 0


async def run_publisher(broker: Broker, options: CLIOptions) -> int:
    async with broker.publisher(
        PublisherOptions(
            name=options.exchange.name,
            prefetch_count=1,
        )
        if options.exchange is not None
        else PublisherOptions(prefetch_count=1),
        serializer=options.serializer,
    ) as publisher:
        async for body in read_lines(options.input):
            message = AppMessage(body=body, routing_key=options.routing_key)
            result = await publisher.publish(message)

            match result:
                case True | None:
                    pass

                case False:
                    await write_lines(options.err, f"failed to send: {message!r}")

                case _:
                    t.assert_never(result)

        return 0


async def consume_message(out: t.IO[bytes], message: Message[object]) -> None:
    await write_lines(out, repr(message))


async def read_lines(in_: t.IO[bytes]) -> t.AsyncIterator[bytes]:
    loop = asyncio.get_running_loop()

    while not in_.closed:
        line = await loop.run_in_executor(None, in_.readline)
        if not line:
            break

        yield line


async def write_lines(out: t.IO[bytes], *lines: str) -> None:
    loop = asyncio.get_running_loop()

    for line in lines:
        await loop.run_in_executor(None, out.write, f"{line}\n".encode())


def parse_options(args: t.Sequence[str]) -> CLIOptions:
    options = build_parser().parse_args(args, namespace=CLIOptions())

    options.input = sys.stdin.buffer
    options.output = sys.stdout.buffer
    options.err = sys.stderr.buffer

    return options


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="""Exchange messages with broker.""",
    )
    parser.add_argument(
        "url",
        type=URL,
        help="broker url to connect to",
    )
    parser.add_argument(
        "routing_key",
        type=str,
        help="routing key to use for message publishing / consumption",
    )
    parser.add_argument(
        "-e",
        "--exchange",
        type=parse_exchange_name,
        help="exchange name to connect to",
        default=None,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-c",
        "--consumer",
        dest="kind",
        action="store_const",
        const="consumer",
        help="consumer mode. Print received messages to stdout.",
    )
    group.add_argument(
        "-p",
        "--publisher",
        dest="kind",
        action="store_const",
        const="publisher",
        help="publisher mode. Send messages from stdin.",
    )

    parser.add_argument(
        "-s",
        "--serializer",
        type=parse_serializer,
        help="customize serializer. Available options: json, protobuf:{entrypoint to protobuf message class}, "
        "{entrypoint to serializer class}",
        default=IdentSerializer(),
    )

    return parser


def parse_exchange_name(value: str) -> ExchangeOptions:
    return ExchangeOptions(name=value)


def parse_serializer(value: str | None) -> Serializer[object, BinaryMessage]:
    if value is None:
        return IdentSerializer()

    match value.split(":", maxsplit=1):
        case ["raw"]:
            return IdentSerializer()

        case ["json"]:
            from brokrpc.serializer.json import JSONSerializer

            return JSONSerializer()

        case ["protobuf", msg_type]:
            from google.protobuf.message import Message as ProtobufMessage

            from brokrpc.serializer.protobuf import JSONProtobufSerializer

            return JSONProtobufSerializer(load_class(ProtobufMessage, msg_type))

        case _:
            return load_instance(Serializer[Message[object], BinaryMessage], value)


if __name__ == "__main__":
    sys.exit(asyncio.run(run(parse_options(sys.argv[1:]))))
