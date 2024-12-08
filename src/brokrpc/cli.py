from __future__ import annotations

import abc
import asyncio
import json
import sys
import typing as t
from argparse import ArgumentParser
from dataclasses import dataclass, replace
from datetime import timedelta
from uuid import uuid4

from aiofiles import stderr_bytes, stdin_bytes, stdout_bytes
from yarl import URL

from brokrpc.abc import Consumer, Serializer
from brokrpc.broker import Broker, parse_options
from brokrpc.message import AppMessage, BinaryMessage, Message, PackedMessage
from brokrpc.middleware import AbortBadMessageMiddleware
from brokrpc.model import BrokerError, ConsumerResult
from brokrpc.options import BindingOptions, BrokerOptions, ExchangeOptions, PublisherOptions, QueueOptions
from brokrpc.plugin import Loader
from brokrpc.retry import ConstantDelay, DelayRetryStrategy, ExponentialDelay, MultiplierDelay
from brokrpc.serializer.ident import IdentSerializer


@dataclass()
class Options:
    url: URL | None = None
    retry_delay: timedelta | None = None
    retry_delay_mode: t.Literal["constant", "multiplier", "exponential"] | None = None
    retry_max_delay: timedelta | None = None
    retries_timeout: timedelta | None = None
    retries_limit: int | None = None
    mode: t.Literal["publish", "consume", "check"] | None = None
    exchange: str | None = None
    routing_key: str | None = None
    encoder: Serializer[BinaryMessage, BinaryMessage] | None = None
    decoder: Serializer[bytes, BinaryMessage] | None = None
    message_mode: t.Literal["body", "wide"] | None = None
    stdin: AsyncStream | None = None
    stdout: AsyncStream | None = None
    stderr: AsyncStream | None = None
    quiet: bool | None = None
    done: asyncio.Event | None = None


class Factory:
    def __init__(self, options: Options) -> None:
        self.__options = options

    def create_app(self) -> App:
        match self.__options.mode:
            case None:
                details = "mode is required"
                raise ValueError(details, self)

            case "publish":
                return self.__create_publisher_app()

            case "consume":
                return self.__create_consumer_app()

            case "check":
                return self.__create_waiter_app()

            case _:
                t.assert_never(self.__options.mode)

    def __create_publisher_app(self) -> PublisherApp:
        if self.__options.routing_key is None:
            details = "routing key is required for publisher mode"
            raise ValueError(details, self)

        return PublisherApp(
            broker=self.__create_broker(),
            options=(
                PublisherOptions(
                    name=self.__options.exchange,
                    prefetch_count=1,
                )
            ),
            routing_key=self.__options.routing_key,
            serializer=self.__options.encoder or IdentSerializer(),
            console=self.__create_console(),
        )

    def __create_consumer_app(self) -> ConsumerApp:
        if self.__options.routing_key is None:
            details = "routing key is required for publisher mode"
            raise ValueError(details, self)

        return ConsumerApp(
            broker=self.__create_broker(),
            options=BindingOptions(
                exchange=ExchangeOptions(name=self.__options.exchange),
                binding_keys=[self.__options.routing_key],
                queue=QueueOptions(
                    name=f"{__package__}.{uuid4().hex}",
                    exclusive=True,
                    auto_delete=True,
                    prefetch_count=1,
                ),
            ),
            serializer=self.__options.decoder or IdentSerializer(),
            console=self.__create_console(),
        )

    def __create_waiter_app(self) -> WaiterApp:
        return WaiterApp(
            broker=self.__create_broker(),
            console=self.__create_console(),
        )

    def __create_broker(self) -> Broker:
        return Broker(
            options=self.__create_broker_options(),
            default_publisher_middlewares=[AbortBadMessageMiddleware()],
            default_consumer_middlewares=[AbortBadMessageMiddleware()],
        )

    def __create_console(self) -> Console:
        if self.__options.stdin is None:
            details = "stdin is required"
            raise ValueError(details, self.__options)

        return Console(
            stdin=self.__options.stdin,
            stdout=self.__options.stdout,
            stderr=self.__options.stderr if not self.__options.quiet else None,
        )

    def __create_broker_options(self) -> BrokerOptions:
        if self.__options.url is None:
            details = "url must be set"
            raise ValueError(details, self.__options)

        return replace(
            parse_options(self.__options.url),
            retry_delay=self.__create_retry_delay_strategy(),
            retries_timeout=self.__options.retries_timeout,
            retries_limit=self.__options.retries_limit,
        )

    def __create_retry_delay_strategy(self) -> DelayRetryStrategy | None:
        if self.__options.retry_delay is None:
            return None

        match self.__options.retry_delay_mode:
            case "constant":
                return ConstantDelay(
                    delay=self.__options.retry_delay,
                )

            case "multiplier":
                return MultiplierDelay(
                    delay=self.__options.retry_delay,
                    max_delay=self.__options.retry_max_delay,
                )

            case "exponential":
                return ExponentialDelay(
                    delay=self.__options.retry_delay,
                    max_delay=self.__options.retry_max_delay,
                )

            case None:
                return None

            case _:
                details = "unknown mode"
                raise ValueError(details, self.__options.retry_delay_mode)


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


class Console:
    def __init__(
        self,
        stdin: AsyncStream,
        stdout: AsyncStream | None,
        stderr: AsyncStream | None,
    ) -> None:
        self.__stdin = stdin
        self.__stdout = stdout
        self.__stderr = stderr

    def read(self) -> t.AsyncIterable[bytes]:
        return aiter(self.__stdin)

    async def write(self, *lines: bytes | str) -> None:
        if not lines or self.__stdout is None:
            return

        for line in lines:
            await self.__stdout.write((line if isinstance(line, bytes) else line.encode()) + b"\n")

        await self.__stdout.flush()

    async def log(self, line: str) -> None:
        if self.__stderr is None:
            return

        await self.__stderr.write(line.encode() + b"\n")
        await self.__stderr.flush()

    async def error(self, err: Exception) -> None:
        await self.log(f"error: {err!r}")

    async def close(self) -> None:
        await self.__stdin.close()


class App(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def run(self) -> int:
        raise NotImplementedError


class PublisherApp(App):
    def __init__(
        self,
        broker: Broker,
        console: Console,
        options: PublisherOptions,
        routing_key: str,
        serializer: Serializer[BinaryMessage, BinaryMessage],
    ) -> None:
        self.__broker = broker
        self.__console = console
        self.__options = options
        self.__routing_key = routing_key
        self.__serializer = serializer

    async def run(self) -> int:
        async with (
            self.__broker,
            self.__broker.publisher(options=self.__options, serializer=self.__serializer) as publisher,
        ):
            async for body in self.__console.read():
                message = AppMessage(body=body, routing_key=self.__routing_key)
                result = await publisher.publish(message)

                match result:
                    case True | None:
                        await self.__console.log("message sent")

                    case False:
                        await self.__console.log(f"failed to send: {body!r}")

                    case _:
                        t.assert_never(result)

            return 0


class ConsumerApp(App, Consumer[bytes, ConsumerResult]):
    def __init__(
        self,
        broker: Broker,
        options: BindingOptions,
        serializer: Serializer[bytes, BinaryMessage],
        console: Console,
    ) -> None:
        self.__broker = broker
        self.__options = options
        self.__serializer = serializer
        self.__console = console

    async def run(self) -> int:
        async with (
            self.__broker,
            self.__broker.consumer(
                consumer=self,
                options=self.__options,
                serializer=self.__serializer,
            ),
        ):
            async for _ in self.__console.read():
                pass

            return 0

    async def consume(self, message: bytes) -> ConsumerResult:
        await self.__console.write(message)
        await self.__console.log("message consumed")
        return True


class WaiterApp(App):
    def __init__(self, broker: Broker, console: Console) -> None:
        self.__broker = broker
        self.__console = console

    async def run(self) -> int:
        try:
            async with self.__broker:
                pass

        except BrokerError as err:
            await self.__console.error(err)
            return 1

        else:
            await self.__console.log("successfully connected to broker")
            return 0

    # TODO: implement
    async def stop(self) -> None:
        pass


class Parser:
    def __init__(self, *, exit_on_error: bool = True) -> None:
        self.__impl = self.__build_impl(exit_on_error=exit_on_error)

    def parse(self, args: t.Sequence[str] | None = None, default: Options | None = None) -> Options:
        return self.__impl.parse_args(
            args=args,
            namespace=default
            if default is not None
            else Options(
                stdin=stdin_bytes,
                stdout=stdout_bytes,
                stderr=stderr_bytes,
                done=asyncio.Event(),
            ),
        )

    @classmethod
    def __build_impl(cls, *, exit_on_error: bool) -> ArgumentParser:
        parser = ArgumentParser(
            description="communicate with broker",
            exit_on_error=exit_on_error,
        )
        parser.register("type", "seconds", cls.__parse_seconds)
        cls.__add_main_opts(parser)

        modes_subparser = parser.add_subparsers(
            title="modes",
            required=True,
        )

        # publisher mode
        publisher_parser = modes_subparser.add_parser(
            name="publish",
            help="publish messages from stdin",
            exit_on_error=exit_on_error,
        )

        publisher_parser.set_defaults(mode="publish")
        cls.__add_routing_opts(publisher_parser)
        publisher_parser.add_argument(
            "--encoder",
            # TODO: find a way to register custom type with `parser.register`
            type=cls.__parse_encoder,
            help="""
            set message encoder;
            predefined: raw, json, protobuf:{entrypoint to protobuf message class}, {entrypoint to serializer callable};
            default: raw (keep binary data from body as is)
            """,
            default=None,
        )

        # consumer mode
        consumer_parser = modes_subparser.add_parser(
            name="consume",
            help="consume messages to stdout",
            exit_on_error=exit_on_error,
        )
        consumer_parser.set_defaults(mode="consume")
        cls.__add_routing_opts(consumer_parser)
        consumer_parser.add_argument(
            "--decoder",
            # TODO: find a way to register custom type with `parser.register`
            type=cls.__parse_decoder,
            help="""
            set message decoder;
            predefined: raw, repr, json, protobuf:{entrypoint to protobuf message class},
            {entrypoint to serializer callable};
            default: raw (keep binary data from body as is)
            """,
            default=None,
        )

        # checker mode
        checker_parser = modes_subparser.add_parser(
            name="check",
            help="check connection with broker",
            exit_on_error=exit_on_error,
        )
        checker_parser.set_defaults(mode="check")

        return parser

    @classmethod
    def __add_main_opts(cls, parser: ArgumentParser) -> None:
        parser.add_argument(
            "url",
            type=URL,
            help="broker url to connect to",
        )

        retry_group = parser.add_argument_group(
            title="retry options",
            description="customize retry logic when connecting to broker",
        )
        retry_group.add_argument(
            "--retry-delay",
            type="seconds",
            default=None,
            metavar="S",
        )
        retry_group.add_argument(
            "--retry-delay-mode",
            type=str,
            choices=["constant", "multiplier", "exponential"],
            default=None,
        )
        retry_group.add_argument(
            "--retry-max-delay",
            type="seconds",
            default=None,
            metavar="S",
        )
        retry_group.add_argument(
            "--retries-timeout",
            type="seconds",
            default=None,
            metavar="S",
        )
        retry_group.add_argument(
            "--retries-limit",
            type=int,
            default=None,
            metavar="N",
        )

        parser.add_argument(
            "-q",
            "--quiet",
            dest="quiet",
            action="store_true",
            help="don't print to stderr; default: %(default)s",
            default=False,
        )

    @classmethod
    def __add_routing_opts(cls, parser: ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="routing options",
        )

        group.add_argument(
            "routing_key",
            type=str,
            help="set routing key to use for messages",
            default=None,
        )
        group.add_argument(
            "-e",
            "--exchange",
            type=str,
            help="exchange name to connect to",
            default=None,
        )

    @classmethod
    def __parse_seconds(cls, value: str) -> timedelta:
        return timedelta(seconds=float(value))

    @classmethod
    def __parse_encoder(cls, value: str) -> Serializer[BinaryMessage, BinaryMessage] | None:
        match value.split(":", maxsplit=1):
            case ["raw"]:
                return IdentSerializer()

            case ["json"]:
                return cls.__create_json_encoder()

            case ["protobuf", msg_type]:
                return cls.__create_protobuf_encoder(msg_type)

            case _:
                return Loader[Serializer[BinaryMessage, BinaryMessage]]().load_instance(value)

    @classmethod
    def __create_protobuf_encoder(cls, msg: str) -> Serializer[BinaryMessage, BinaryMessage]:
        from google.protobuf.message import Message as ProtobufMessage

        from brokrpc.serializer.protobuf import ProtobufSerializer

        message_class = Loader[ProtobufMessage]().load_class(msg)

        class ProtobufEncoder(Serializer[BinaryMessage, BinaryMessage]):
            def __init__(self, inner: Serializer[Message[ProtobufMessage], BinaryMessage]) -> None:
                self.__inner = inner

            def dump_message(self, message: BinaryMessage) -> BinaryMessage:
                return self.__inner.dump_message(PackedMessage(original=message, body=json.loads(message.body)))

            def load_message(self, message: BinaryMessage) -> BinaryMessage:
                return message

        return ProtobufEncoder(ProtobufSerializer(message_class))

    @classmethod
    def __create_json_encoder(cls) -> Serializer[BinaryMessage, BinaryMessage]:
        from brokrpc.serializer.json import JSONSerializer

        class JSONEncoder(Serializer[BinaryMessage, BinaryMessage]):
            def __init__(self, inner: Serializer[Message[object], BinaryMessage]) -> None:
                self.__inner = inner

            def dump_message(self, message: BinaryMessage) -> BinaryMessage:
                return self.__inner.dump_message(PackedMessage(original=message, body=json.loads(message.body)))

            def load_message(self, message: BinaryMessage) -> BinaryMessage:
                return message

        return JSONEncoder(JSONSerializer())

    @classmethod
    def __parse_decoder(cls, value: str) -> Serializer[bytes, BinaryMessage] | None:
        match value.split(":", maxsplit=1):
            case ["raw"]:
                return cls.__create_raw_decoder()

            case ["repr"]:
                return cls.__create_repr_decoder()

            case ["json"]:
                return cls.__create_json_decoder()

            case ["protobuf", msg_type]:
                return cls.__create_protobuf_decoder(msg_type)

            case _:
                return Loader[Serializer[bytes, BinaryMessage]]().load_instance(value)

    @staticmethod
    def __create_raw_decoder() -> Serializer[bytes, BinaryMessage]:
        class RawDecoder(Serializer[bytes, BinaryMessage]):
            def dump_message(self, message: bytes) -> BinaryMessage:
                raise NotImplementedError

            def load_message(self, message: BinaryMessage) -> bytes:
                return message.body

        return RawDecoder()

    @staticmethod
    def __create_repr_decoder() -> Serializer[bytes, BinaryMessage]:
        class ReprDecoder(Serializer[bytes, BinaryMessage]):
            def dump_message(self, message: bytes) -> BinaryMessage:
                raise NotImplementedError

            def load_message(self, message: BinaryMessage) -> bytes:
                return repr(message).encode()

        return ReprDecoder()

    @staticmethod
    def __create_json_decoder() -> Serializer[bytes, BinaryMessage]:
        from brokrpc.serializer.json import JSONSerializer

        class JSONDecoder(Serializer[bytes, BinaryMessage]):
            def __init__(self, inner: Serializer[Message[object], BinaryMessage]) -> None:
                self.__inner = inner

            def dump_message(self, message: bytes) -> BinaryMessage:
                raise NotImplementedError

            def load_message(self, message: BinaryMessage) -> bytes:
                json_message = self.__inner.load_message(message)
                return json.dumps(json_message.body, indent=None, separators=(",", ":")).encode()

        return JSONDecoder(JSONSerializer())

    @staticmethod
    def __create_protobuf_decoder(msg: str) -> Serializer[bytes, BinaryMessage]:
        from google.protobuf.message import Message as ProtobufMessage

        from brokrpc.serializer.protobuf import ProtobufSerializer

        message_class = Loader[ProtobufMessage]().load_class(msg)

        class ProtobufDecoder(Serializer[bytes, BinaryMessage]):
            def __init__(self, inner: Serializer[Message[ProtobufMessage], BinaryMessage]) -> None:
                self.__inner = inner

            def dump_message(self, message: bytes) -> BinaryMessage:
                raise NotImplementedError

            def load_message(self, message: BinaryMessage) -> bytes:
                protobuf_message = self.__inner.load_message(message)
                return str(protobuf_message.body).encode()

        return ProtobufDecoder(ProtobufSerializer(message_class))


def main() -> None:
    app = Factory(Parser().parse()).create_app()
    sys.exit(asyncio.run(app.run()))


if __name__ == "__main__":
    main()
