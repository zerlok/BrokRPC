import typing as t

import pytest
from protomq.connection import Connection


@pytest.fixture()
def rabbitmq_url() -> str:
    return "amqp://guest:guest@localhost:5672/"


@pytest.fixture()
async def rabbitmq_connection(rabbitmq_url: str) -> t.AsyncIterator[Connection]:
    async with Connection(rabbitmq_url) as conn:
        yield conn
