import asyncio
import typing as t
from collections import defaultdict
from contextlib import asynccontextmanager

from brokrpc.abc import BinaryConsumer, BinaryPublisher, BoundConsumer, BrokerDriver, Consumer, Publisher
from brokrpc.model import ConsumerResult, PublisherResult
from brokrpc.options import BindingOptions, PublisherOptions


class MessageStorage:
    def __init__(
        self,
    ) -> None:
        self.__messages: t.List[object] = []
        self.__condition = asyncio.Condition()

    async def add(self, message: object) -> None:
        async with self.__condition:
            self.__messages.append(message)
            self.__condition.notify_all()

    async def wait_for(self, predicate: t.Callable[[], bool]) -> t.Sequence[object]:
        async with self.__condition:
            await self.__condition.wait_for(predicate)
            return list(self.__messages)


type PublisherSideEffect = t.Callable[[object], t.Awaitable[PublisherResult]]
type ConsumerSideEffect = t.Callable[[object], t.Awaitable[ConsumerResult]]


class StubPublisher(Publisher[object, PublisherResult], MessageStorage):
    def __init__(
        self,
        options: PublisherOptions | None = None,
        side_effect: PublisherSideEffect | None = None,
    ) -> None:
        super().__init__()
        self.__options = options
        self.__side_effect = side_effect

    async def publish(self, message: object) -> PublisherResult:
        if self.__side_effect is not None:
            result = await self.__side_effect(message)
        else:
            result = None

        await self.add(message)

        return result


class StubConsumer(Consumer[object, ConsumerResult], MessageStorage):
    def __init__(
        self,
        side_effect: ConsumerSideEffect | None = None,
    ) -> None:
        super().__init__()
        self.__side_effect = side_effect

    async def consume(self, message: object) -> ConsumerResult:
        if self.__side_effect is not None:
            result = await self.__side_effect(message)
        else:
            result = None

        await self.add(message)

        return result


class StubBoundConsumer(BoundConsumer):
    def __init__(self, consumer: BinaryConsumer, options: BindingOptions) -> None:
        self.__consumer = consumer
        self.__options = options

    @property
    def consumer(self) -> BinaryConsumer:
        return self.__consumer

    def is_alive(self) -> bool:
        return True

    def get_options(self) -> BindingOptions:
        return self.__options


class StubBrokerDriver(BrokerDriver):
    def __init__(
        self,
        provide_publisher: t.Callable[[StubPublisher], t.AsyncContextManager[BinaryPublisher]] | None = None,
        provide_consumer: t.Callable[[StubBoundConsumer], t.AsyncContextManager[BoundConsumer]] | None = None,
    ) -> None:
        self.__publisher_provider = provide_publisher or self.__provide_publisher
        self.__consumer_provider = provide_consumer or self.__provide_consumer
        self.__publishers: t.DefaultDict[PublisherOptions, t.List[BinaryPublisher]] = defaultdict(list)
        self.__consumers: t.DefaultDict[BindingOptions, t.List[BinaryConsumer]] = defaultdict(list)

    @property
    def publishers(self) -> t.Mapping[PublisherOptions, t.Sequence[BinaryPublisher]]:
        return self.__publishers

    @property
    def consumers(self) -> t.Mapping[BindingOptions, t.Sequence[BinaryConsumer]]:
        return self.__consumers

    def provide_publisher(self, options: PublisherOptions | None = None) -> t.AsyncContextManager[BinaryPublisher]:
        return self.__publisher_provider(StubPublisher(options))

    def bind_consumer(self, consumer: BinaryConsumer, options: BindingOptions) -> t.AsyncContextManager[BoundConsumer]:
        return self.__consumer_provider(StubBoundConsumer(consumer, options))

    @asynccontextmanager
    async def __provide_publisher(self, publisher: BinaryPublisher) -> t.AsyncIterator[BinaryPublisher]:
        yield publisher

    @asynccontextmanager
    async def __provide_consumer(self, consumer: BoundConsumer) -> t.AsyncIterator[BoundConsumer]:
        yield consumer
