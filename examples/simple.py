import asyncio

from protomq.connection import Connection
from protomq.message import Message
from protomq.options import BindingOptions, PublisherOptions
from protomq.serializer import JSONSerializer


async def main() -> None:
    received = asyncio.Future()

    def consume_message(msg: Message[object]) -> None:
        received.set_result(msg)

    url = "amqp://guest:guest@localhost:5672/"
    message_to_publish = Message(body={"content": "hello world"}, routing_key="test-simple")
    serializer = JSONSerializer()
    pub_options = PublisherOptions(name="simple-test")
    bind_options = BindingOptions(exchange=pub_options, binding_keys=(message_to_publish.routing_key,))

    async with (
        Connection(url) as conn,
        conn.consumer(consume_message, bind_options, serializer=serializer),
        conn.publisher(pub_options, serializer=serializer) as pub,
    ):
        await pub.publish(message_to_publish)
        received_message = await asyncio.wait_for(received, 10.0)

        assert received_message.body == message_to_publish.body
        print(repr(received_message))


if __name__ == "__main__":
    asyncio.run(main())
