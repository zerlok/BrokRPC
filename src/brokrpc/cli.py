import abc
import asyncio
import sys
import typing as t
from argparse import ArgumentParser
from functools import partial
from signal import Signals
from uuid import uuid4

from aiofiles import stderr_bytes, stdin_bytes, stdout_bytes
from yarl import URL

from brokrpc.abc import Serializer
from brokrpc.broker import Broker
from brokrpc.message import BinaryMessage, Message, create_message
from brokrpc.middleware import AbortBadMessageMiddleware
from brokrpc.options import BindingOptions, ExchangeOptions, PublisherOptions, QueueOptions
from brokrpc.plugin import Loader
from brokrpc.serializer.ident import IdentSerializer


class AsyncStream(t.Protocol):
    @abc.abstractmethod
    def __aiter__(self) -> t.AsyncIterator[bytes]:
        raise NotImplementedError

    @abc.abstractmethod
    async def __anext__(self) -> bytes:
        raise NotImplementedError

    @abc.abstractmethod
    async def readlines(self) -> t.Sequence[bytes]:
        raise NotImplementedError

    @abc.abstractmethod
    async def write(self, _: bytes, /) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    async def flush(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class CLIOptions:
    url: URL
    exchange: ExchangeOptions | None
    routing_key: str
    kind: t.Literal["consumer", "publisher"]
    serializer: Serializer[Message[object], BinaryMessage]
    output_mode: t.Literal["wide", "body"]
    quiet: bool
    input: AsyncStream
    output: AsyncStream
    err: AsyncStream
    done: asyncio.Event

    def __str__(self) -> str:
        data = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())
        return f"{self.__class__.__name__}({data})"

    __repr__ = __str__


async def run(options: CLIOptions) -> int:
    async with (
        Broker(
            options.url,
            default_publisher_middlewares=[
                AbortBadMessageMiddleware(),
            ],
            default_consumer_middlewares=[
                AbortBadMessageMiddleware(),
            ],
        ) as broker,
    ):
        match options.kind:
            case "consumer":
                return await run_consumer(broker, options)

            case "publisher":
                return await run_publisher(broker, options)

            case _:
                t.assert_never(options.kind)


async def run_consumer(broker: Broker, options: CLIOptions) -> int:
    async with broker.consumer(
        partial(consume_message_wide, options.output)
        if options.output_mode == "wide"
        else partial(consume_message_body, options.output),
        BindingOptions(
            exchange=options.exchange,
            binding_keys=[options.routing_key],
            queue=QueueOptions(
                name=f"{__package__}.{uuid4().hex}",
                exclusive=True,
                auto_delete=True,
                prefetch_count=1,
            ),
        ),
        serializer=options.serializer,
    ):
        await options.done.wait()

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
        async for body in options.input:
            message = create_message(body=body, routing_key=options.routing_key)
            result = await publisher.publish(message)

            match result:
                case True | None:
                    if not options.quiet:
                        await options.err.write(b"sent\n")
                        await options.err.flush()

                case False:
                    if not options.quiet:
                        await options.err.write(f"failed to send: {message!r}\n".encode())
                        await options.err.flush()

                case _:
                    t.assert_never(result)

        return 0


async def consume_message_wide(out: AsyncStream, message: Message[object]) -> None:
    await out.write(f"{message!r}\n".encode())
    await out.flush()


async def consume_message_body(out: AsyncStream, message: Message[bytes]) -> None:
    await out.write(message.body + b"\n")
    await out.flush()


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
        help="customize serializer. Available options: json, protobuf:{plugin to protobuf message class}, "
        "{plugin to serializer class}. Default: none (pass binary data to body as is).",
        default=IdentSerializer(),
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        dest="output_mode",
        choices=["wide", "body"],
        help="customize output mode. Available options: wide, body. Default: body",
        default="body",
    )

    parser.add_argument(
        "-q",
        "--quiet",
        dest="quiet",
        action="store_const",
        const=True,
        help="don't print to stderr",
        default=False,
    )

    return parser


def parse_exchange_name(value: str) -> ExchangeOptions:
    return ExchangeOptions(name=value)


# NOTE: allow any type to be loaded by serializer (used by consumer).
def parse_serializer(  # type: ignore[misc]
    value: str | None,
) -> Serializer[t.Any, BinaryMessage]:
    if value is None:
        return IdentSerializer[BinaryMessage]()

    match value.split(":", maxsplit=1):
        case ["raw"]:
            return IdentSerializer()

        case ["json"]:
            from brokrpc.serializer.json import JSONSerializer

            return JSONSerializer()

        case ["protobuf", msg_type]:
            from google.protobuf.message import Message as ProtobufMessage

            from brokrpc.serializer.protobuf import JSONProtobufSerializer

            return JSONProtobufSerializer(Loader[ProtobufMessage]().load_class(msg_type))

        case _:
            return Loader[Serializer[t.Any, BinaryMessage]]().load_instance(value)


def main() -> None:
    options = build_parser().parse_args(namespace=CLIOptions())

    options.input = stdin_bytes
    options.output = stdout_bytes
    options.err = stderr_bytes
    done = options.done = asyncio.Event()

    async def _main() -> int:
        for sig in (Signals.SIGINT, Signals.SIGTERM):
            asyncio.get_running_loop().add_signal_handler(sig, done.set)

        return await run(options)

    sys.exit(asyncio.run(_main()))


if __name__ == "__main__":
    main()
