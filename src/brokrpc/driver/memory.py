from __future__ import annotations

import asyncio
import typing as t
from asyncio import TaskGroup
from collections import defaultdict
from contextlib import asynccontextmanager, nullcontext, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import count

if t.TYPE_CHECKING:
    from brokrpc.options import BindingOptions, BrokerOptions, PublisherOptions

from brokrpc.abc import BinaryConsumer, BinaryPublisher, BoundConsumer, BrokerDriver, Publisher
from brokrpc.message import BinaryMessage
from brokrpc.model import PublisherResult
from brokrpc.queue import Queue
from brokrpc.stringify import to_str_obj


@dataclass(kw_only=True)
class RegisteredQueueContext:
    options: BindingOptions
    queue: Queue[BinaryMessage]
    refs_count: int


class InMemoryQueueRegistry(t.AsyncContextManager["InMemoryQueueRegistry"]):
    def __init__(self) -> None:
        self.__contexts = dict[str | None, RegisteredQueueContext]()
        self.__bindings = defaultdict[tuple[str | None, str], set[Queue[BinaryMessage]]](set)

    async def __aenter__(self) -> t.Self:
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.__bindings.clear()

        try:
            await asyncio.gather(*(context.queue.close(force=True) for context in self.__contexts.values()))

        finally:
            self.__contexts.clear()

    @asynccontextmanager
    async def acquire(self, options: BindingOptions) -> t.AsyncIterator[Queue[BinaryMessage]]:
        context = self.__contexts.get(options.queue.name if options.queue is not None else None) or self.__create(
            options
        )

        self.__acquire(context)
        try:
            yield context.queue

        finally:
            await self.__release(context)

    def find_by_key(self, exchange: str | None, key: str) -> t.Collection[Queue[BinaryMessage]]:
        return self.__bindings[(exchange, key)]

    def __create(self, options: BindingOptions) -> RegisteredQueueContext:
        queue = self.__contexts[options.queue.name if options.queue is not None else None] = RegisteredQueueContext(
            options=options,
            queue=Queue(),
            refs_count=0,
        )

        return queue

    def __acquire(self, context: RegisteredQueueContext) -> None:
        context.refs_count += 1

        for exchange, key in self.__iter_keys(context):
            self.__bindings[(exchange, key)].add(context.queue)

    async def __release(self, context: RegisteredQueueContext) -> None:
        context.refs_count -= 1
        if context.refs_count > 0:
            return

        for exchange, key in self.__iter_keys(context):
            self.__bindings[(exchange, key)].discard(context.queue)

        self.__contexts.pop(context.options.queue.name if context.options.queue is not None else None, None)
        await context.queue.close()

    def __iter_keys(self, context: RegisteredQueueContext) -> t.Iterable[tuple[str | None, str]]:
        exchange = (
            context.options.exchange.name
            if context.options.exchange is not None and context.options.exchange.name is not None
            else None
        )

        return ((exchange, key) for key in context.options.binding_keys)


class InMemoryFanoutPublisher(Publisher[BinaryMessage, PublisherResult]):
    def __init__(
        self,
        registry: InMemoryQueueRegistry,
        options: PublisherOptions | None,
        message_id_gen: t.Callable[[], str | None],
        now: t.Callable[[], datetime | None],
    ) -> None:
        self.__registry = registry
        self.__options = options
        self.__message_id_gen = message_id_gen
        self.__now = now

    async def publish(self, message: BinaryMessage) -> PublisherResult:
        queues = self.__registry.find_by_key(
            exchange=message.exchange
            if message.exchange is not None
            else self.__options.name
            if self.__options is not None
            else None,
            key=message.routing_key,
        )
        if not queues:
            return False

        await asyncio.gather(*(queue.write(message) for queue in queues))

        return True


class InMemoryBoundConsumer(BoundConsumer):
    def __init__(
        self,
        options: BindingOptions,
        task: asyncio.Task[None],
    ) -> None:
        self.__options = options
        self.__task = task

    def is_alive(self) -> bool:
        return not self.__task.done()

    def get_options(self) -> BindingOptions:
        return self.__options


class InMemoryBrokerDriver(BrokerDriver):
    @classmethod
    @asynccontextmanager
    async def setup(cls, _: BrokerOptions) -> t.AsyncIterator[InMemoryBrokerDriver]:
        async with TaskGroup() as tasks, InMemoryQueueRegistry() as queues:
            yield cls(tasks, queues)

    def __init__(self, tasks: TaskGroup, queues: InMemoryQueueRegistry) -> None:
        self.__tasks = tasks
        self.__queues = queues

        self.__message_id = iter(count())
        self.__bound_consumers = set[InMemoryBoundConsumer]()

    def __str__(self) -> str:
        return to_str_obj(self)

    def provide_publisher(self, options: PublisherOptions | None = None) -> t.AsyncContextManager[BinaryPublisher]:
        return nullcontext(InMemoryFanoutPublisher(self.__queues, options, self.__gen_message_id, self.__get_now))

    @asynccontextmanager
    async def bind_consumer(self, consumer: BinaryConsumer, options: BindingOptions) -> t.AsyncIterator[BoundConsumer]:
        async with self.__queues.acquire(options) as queue:
            task = self.__tasks.create_task(self.__consume(queue, consumer), name=str(consumer))

            bound_consumer = InMemoryBoundConsumer(options, task)
            self.__bound_consumers.add(bound_consumer)
            try:
                yield bound_consumer

            finally:
                if not task.done():
                    task.cancel("teardown consumer")
                    with suppress(asyncio.CancelledError):
                        await task

                self.__bound_consumers.discard(bound_consumer)

    async def __consume(self, queue: Queue[BinaryMessage], consumer: BinaryConsumer) -> None:
        async for message in queue.read():
            # TODO: handle consumer result
            await consumer.consume(message)

    def __gen_message_id(self) -> str:
        return str(next(self.__message_id))

    def __get_now(self) -> datetime:
        return datetime.now(tz=UTC)
