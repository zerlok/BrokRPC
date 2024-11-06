import asyncio
from datetime import timedelta
from pprint import pprint

from protomq.connection import Connection
from protomq.message import Message
from protomq.middleware import RetryOnErrorConsumerMiddleware
from protomq.options import BindingOptions, ExchangeOptions, QueueOptions
from protomq.serializer import JSONSerializer


async def main() -> None:
    # define app consumer
    consumed = asyncio.Future()

    def consume_message(msg: Message[object]) -> None:
        consumed.set_result(msg)

    # common AMQP & serialization settings
    routing_key = "test-greeting"
    serializer = JSONSerializer()

    async with (
        # create a connection
        Connection(
            dsn="amqp://guest:guest@localhost:5672/",
            default_exchange=ExchangeOptions(name="simple-test-app"),
            default_consumer_middlewares=[RetryOnErrorConsumerMiddleware((Exception,), timedelta(seconds=3.0))],
        ) as conn,
        # start app consumer
        conn.consumer(
            consume_message,
            BindingOptions(binding_keys=(routing_key,), queue=QueueOptions(name="greetings-queue")),
            serializer=serializer,
        ) as consumer,
        # get a publisher
        conn.publisher(serializer=serializer) as pub,
    ):
        # publish app message
        message_to_publish = Message(body={"content": "hello world"}, routing_key=routing_key)
        await pub.publish(message_to_publish)

        # wait for consumed message & do checks
        assert consumer.is_alive()
        consumed_message = await asyncio.wait_for(consumed, 10.0)
        assert consumed_message.body == message_to_publish.body
        pprint(consumed_message)


if __name__ == "__main__":
    asyncio.run(main())
