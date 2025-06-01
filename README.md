# BrokRPC

[![Latest Version](https://img.shields.io/pypi/v/BrokRPC.svg)](https://pypi.python.org/pypi/BrokRPC)
[![Python Supported versions](https://img.shields.io/pypi/pyversions/BrokRPC.svg)](https://pypi.python.org/pypi/BrokRPC)
[![MyPy Strict](https://img.shields.io/badge/mypy-strict-blue)](https://mypy.readthedocs.io/en/stable/getting_started.html#strict-mode-and-configuration)
[![Test Coverage](https://codecov.io/gh/zerlok/BrokRPC/branch/main/graph/badge.svg)](https://codecov.io/gh/zerlok/BrokRPC)
[![Downloads](https://img.shields.io/pypi/dm/BrokRPC.svg)](https://pypistats.org/packages/BrokRPC)
[![GitHub stars](https://img.shields.io/github/stars/zerlok/BrokRPC)](https://github.com/zerlok/BrokRPC/stargazers)

BrokRPC (**Brok**er **R**emote **P**rocedure **C**all) is a framework for gRPC like server-client communication over 
message brokers.

## key features

* strict typing (even `disallow_any_expr=true`)
* same protobuf structures as in gRPC
* similar calls as in gRPC
  * unary-unary
  * (TODO) unary-stream
  * (TODO) stream-unary
  * (TODO) stream-stream
* declarative style, abstract from broker commands (such as declare_exchange / queue_bind)
* publisher & consumer middlewares
* message serializers

## codegen

You can generate python code for server & client from `.proto` files. 
The [pyprotostuben](https://github.com/zerlok/pyprotostuben) project provides protoc plugin `protoc-gen-brokrpc`. See 
pyprotostuben project example for more details.

You may configure codegen output using protobuf extensions from [buf schema registry](https://buf.build/zerlok/brokrpc).

## supported brokers & protocols

* [AMQP](https://www.rabbitmq.com/tutorials/amqp-concepts)
  * [aiormq](https://github.com/mosquito/aiormq)
* (TODO) redis
* (TODO) kafka
* (TODO) NATS

## usage

[pypi package](https://pypi.python.org/pypi/BrokRPC)

install with your favorite python package manager

```shell
pip install BrokRPC[aiormq]
```

### Broker

use Broker as high level API to create consumers & publishers

```python
from brokrpc.broker import Broker

# create & connect to broker with specified params
async with Broker(...) as broker:
    assert broker.is_connected
    # work with broker
    ...
```

### Consumer

```python
import asyncio
from brokrpc.broker import Broker
from brokrpc.message import Message
from brokrpc.options import BindingOptions, QueueOptions, ExchangeOptions
from brokrpc.serializer.json import JSONSerializer


async def register_consumer(broker: Broker) -> None:
    # define consumer function (you also can use async function & `Consumer` interface).
    def consume_binary_message(message: Message[bytes]) -> None:
        print(message)
  
    # consumer is not attached yet
  
    async with broker.consumer(consume_binary_message, BindingOptions(binding_keys=["my-consumer"])):
        # in this code block consumer is attached to broker and can receive messages
        ...
  
    # outside CM consumer is detached from broker and cannot receive messages
  
    async def consume_json(message: Message[object]) -> bool:
        obj = message.body
        if not isinstance(obj, dict):
            return False
    
        username = obj.get("username")
        if not username:
            return False
        
        print(f"Hello, {username}")
        await asyncio.sleep(1.0)  # simulate long processing
    
        return True
  
    async with broker.consumer(
        consume_json,
        BindingOptions(
            exchange=ExchangeOptions(name="json"),
            binding_keys=["my-json-consumer"],
            queue=QueueOptions(name="jsons", auto_delete=True),
        ),
        serializer=JSONSerializer(),
    ):
        ...
```

### ConsumerMiddlewares

...

### Publisher

```python
from brokrpc.broker import Broker
from brokrpc.message import create_message
from brokrpc.serializer.json import JSONSerializer


async def publish_messages(broker: Broker) -> None:
    async with broker.publisher() as pub:
        # in this code block publisher is attached to broker and can send messages
        await pub.publish(create_message(body=b"this is a binary message", routing_key="test-consumer"))

    async with broker.publisher(serializer=JSONSerializer()) as json_pub:
        await json_pub.publish(create_message(body={"username": "John Smith"}, routing_key="my-json-consumer"))
```

### Publisher Middlewares

...

### Message

* `Message`
* `SomeMessage`
* `EncodedMessage`
* `DecodedMessage`

### Serializer

* `JSONSerializer`
* `ProtobufSerializer`

### RPC Server

* `Server`

### RPC Handler

...

### RPC Client

* `Client`

### RPC Caller

...

## RPC example

### RPC server

run server process with following code

```python
import asyncio

from brokrpc.broker import Broker
from brokrpc.options import ExchangeOptions
from brokrpc.rpc.model import Request
from brokrpc.rpc.server import Server
from brokrpc.serializer.json import JSONSerializer


# define app RPC handler
async def handle_request(request: Request[object]) -> str:
    print(f"{request=!s}")
    print(f"{request.body=}")
    return f"I greet you, {request.body}"


async def main() -> None:
    # create broker & RPC server
    broker = Broker(
        options="amqp://guest:guest@localhost:5672/",
        default_exchange=ExchangeOptions(name="simple-test-app"),
    )
    server = Server(broker)

    # register RPC handler
    server.register_unary_unary_handler(
        func=handle_request,
        routing_key="test-greeting",
        serializer=JSONSerializer(),
    )

    # connect to broker
    async with broker:
        # start RPC server until SIGINT or SIGTERM
        await server.run_until_terminated()


if __name__ == "__main__":
    asyncio.run(main())
```

### RPC client

make a call to RPC server via RPC client with following code

```python
import asyncio

from brokrpc.broker import Broker
from brokrpc.options import ExchangeOptions
from brokrpc.rpc.client import Client
from brokrpc.serializer.json import JSONSerializer


async def main() -> None:
    async with (
        # create & connect to broker
        Broker(
            options="amqp://guest:guest@localhost:5672/",
            default_exchange=ExchangeOptions(name="simple-test-app"),
        ) as broker,
        # create RPC client & get RPC caller
        Client(broker).unary_unary_caller(
            routing_key="test-greeting",
            serializer=JSONSerializer(),
        ) as caller,
    ):
        # publish app message & receive RPC response
        response = await caller.invoke("John")

        print(response)
        print(f"{response.body=}")


if __name__ == "__main__":
    asyncio.run(main())
```
