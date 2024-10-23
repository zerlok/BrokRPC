from __future__ import annotations

import typing as t
import uuid
from contextlib import asynccontextmanager

if t.TYPE_CHECKING:
    from yarl import URL

    from protomq.abc import Driver, Serializer

from protomq.caller import UnaryUnaryCaller
from protomq.consumer import UnaryResponseConsumer
from protomq.driver.connect import connect
from protomq.options import BindingOptions, MessageOptions, PublisherOptions, QueueOptions
from protomq.storage import WaiterStorage


class Client:
    @classmethod
    @asynccontextmanager
    async def connect(cls, url: str | URL) -> t.AsyncIterator[Client]:
        async with connect(url) as driver:
            yield cls(driver)

    def __init__(self, driver: Driver) -> None:
        self.__driver = driver

    @asynccontextmanager
    async def register_unary_unary_caller[U, V](
        self,
        *,
        routing_key: str,
        publisher: PublisherOptions,
        serializer: Serializer[U, V],
        queue: QueueOptions | None = None,
    ) -> t.AsyncIterator[UnaryUnaryCaller[U, V]]:
        caller_id = uuid.uuid4()
        response_key = f"response.{routing_key}.{caller_id.hex}"

        with WaiterStorage[V].create() as waiters:
            async with (
                self.__driver.bind_consumer(
                    consumer=UnaryResponseConsumer(
                        serializer=serializer,
                        waiters=waiters,
                    ),
                    options=BindingOptions(
                        exchange=publisher,
                        binding_keys=[response_key],
                        queue=queue
                        if queue is not None
                        else QueueOptions(
                            name=response_key,
                            durable=False,
                            exclusive=True,
                            auto_delete=True,
                        ),
                    ),
                ),
                self.__driver.provide_publisher(publisher) as requester,
            ):
                yield UnaryUnaryCaller(
                    requester=requester,
                    serializer=serializer,
                    options=MessageOptions(
                        exchange=publisher.name,
                        routing_key=routing_key,
                        reply_to=response_key,
                    ),
                    waiters=waiters,
                )
