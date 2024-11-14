import typing as t

from brokrpc.options import QueueOptions


def create_queue_options(func: t.Any) -> QueueOptions:
    return QueueOptions(
        name=f"{func.__module__}.{func.__name__}",
        durable=False,
        auto_delete=True,
    )
