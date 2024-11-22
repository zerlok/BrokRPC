import asyncio
from datetime import timedelta
from pprint import pprint

from brokrpc.broker import Broker
from brokrpc.message import AppMessage, Message
from brokrpc.middleware import RetryOnErrorConsumerMiddleware
from brokrpc.options import BindingOptions, ExchangeOptions, QueueOptions
from brokrpc.serializer.json import JSONSerializer


async def main() -> None:
    # define app consumer
    consumed = asyncio.Future[Message[object]]()

    def consume_message(msg: Message[object]) -> None:
        consumed.set_result(msg)

    # common AMQP & serialization settings
    routing_key = "test-greeting"
    serializer = JSONSerializer()

    async with (
        # create a connection to broker
        Broker(
            options="amqp://guest:guest@localhost:5672/",
            default_exchange=ExchangeOptions(name="simple-test-app"),
            default_consumer_middlewares=[RetryOnErrorConsumerMiddleware(timedelta(seconds=3.0), (Exception,))],
        ) as broker,
        # start app consumer
        broker.consumer(
            consume_message,
            BindingOptions(binding_keys=(routing_key,), queue=QueueOptions(name="greetings-queue", prefetch_count=4)),
            serializer=serializer,
        ) as consumer,
        # get a publisher
        broker.publisher(serializer=serializer) as pub,
    ):
        # publish app message
        message_to_publish = AppMessage(body={"content": "hello world"}, routing_key=routing_key)
        await pub.publish(message_to_publish)

        # wait for consumed message & do checks
        assert consumer.is_alive()
        consumed_message = await asyncio.wait_for(consumed, 10.0)
        assert consumed_message.body == message_to_publish.body
        pprint(consumed_message)  # noqa: T203


if __name__ == "__main__":
    asyncio.run(main())
