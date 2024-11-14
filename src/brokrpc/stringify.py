import typing as t


def to_str_obj(
    obj: object,
    type_vars: t.Sequence[type[object]] | None = None,
    **kwargs: object,
) -> str:
    parts = [f"{to_str_type(type(obj), type_vars)} object at {hex(id(obj))}"]
    for key, value in kwargs.items():
        parts.append(f"{key}={value}")

    return f"""<{"; ".join(parts)}>"""


def to_str_type(
    type_: type[object],
    type_vars: t.Sequence[type[object]] | None = None,
) -> str:
    value = f"{type_.__module__}.{type_.__name__}"
    return value if not type_vars else f"""{value}[{", ".join(tv.__name__ for tv in type_vars)}]"""
