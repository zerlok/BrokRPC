import os

_verbosity = int(os.getenv("PROTOMQ_VERBOSITY", "0"))


def get_verbosity_level() -> int:
    return _verbosity
