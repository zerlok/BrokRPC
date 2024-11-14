import os
from functools import cache


@cache
def is_debug_enabled() -> bool:
    return os.getenv("BROKRPC_DEBUG", "0").strip().lower() in {"1", "yes", "true"}
