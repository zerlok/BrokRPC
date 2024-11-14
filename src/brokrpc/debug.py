import os
import typing as t

__DEBUG: t.Final = os.getenv("brokrpc_DEBUG", "0").strip().lower() in {"1", "yes", "true"}


def is_debug_enabled() -> bool:
    return __DEBUG
