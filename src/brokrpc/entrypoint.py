import importlib
import typing as t


def load_attribute(entrypoint: str) -> object:
    match entrypoint.split(":", maxsplit=1):
        case [module, attr]:
            try:
                loaded_module = importlib.import_module(module)

            except ImportError as err:
                details = "invalid entrypoint"
                raise ValueError(details, entrypoint) from err

            try:
                obj = getattr(loaded_module, attr)

            except AttributeError as err:
                details = "invalid entrypoint"
                raise ValueError(details, entrypoint) from err

            return obj

        case _:
            details = "invalid entrypoint"
            raise ValueError(details, entrypoint)


def load_class[T](type_: type[T], entrypoint: str) -> type[T]:
    klass = load_attribute(entrypoint)
    if not isinstance(klass, type) or not issubclass(klass, t.get_origin(type_) or type_):
        details = "invalid class type"
        raise TypeError(details, klass)

    return t.cast(type[T], klass)


def load_instance[T](type_: type[T], entrypoint: str) -> T:
    factory = load_attribute(entrypoint)
    if not callable(factory):
        details = "invalid factory type"
        raise TypeError(details, factory)

    obj = factory()
    if not isinstance(obj, t.get_origin(type_) or type_):
        details = "invalid instance type"
        raise TypeError(details, factory)

    return t.cast(T, obj)
