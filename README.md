# protomq

Framework implements gRPC like server-client communication over AMQP protocol.

## why is it good?

* same data structures as in gRPC
* same unary / stream calls as in gRPC


routing_key = do-streaming

* consumer 1
* consumer 2


client => e1, e2, e3, e4, ... en

client send(message{routing_key=do-streaming; element=e1})

e1 -> consumer 1 ==> e2 -> consumer 1 .... 


