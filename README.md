# BrokRPC

Framework implements gRPC like server-client communication over message brokers.

## why is it good?

* same protobuf structures as in gRPC
* same unary / stream calls as in gRPC
* easy to integrate into existing gRPC & message brokers
* declarative style, abstract from broker commands (such as declare_exchange / queue_bind)
* publisher & consumer middlewares
