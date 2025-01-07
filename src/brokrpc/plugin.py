import importlib
import typing as t
from functools import cached_property


@t.final
class Loader[T]:
    def load_class(self, entrypoint: str) -> type[T]:
        klass = self.__load(entrypoint)
        if not self.__check_class(klass):
            details = "invalid class type"
            raise TypeError(details, klass)

        return klass

    def load_instance(self, entrypoint: str) -> T:
        factory = self.__load(entrypoint)
        if not callable(factory):
            details = "invalid factory type"
            raise TypeError(details, factory)

        obj: object = factory()
        if not self.__check_instance(obj):
            details = "invalid instance type"
            raise TypeError(details, obj)

        return obj

    def load_constant(self, entrypoint: str) -> T:
        obj = self.__load(entrypoint)
        if not self.__check_instance(obj):
            details = "invalid instance type"
            raise TypeError(details, obj)

        return obj

    @cached_property
    def __type(self) -> type[T]:
        # NOTE: this hack with `__orig_class__` allows to pass interfaces to `T` (classes that derives `abc.ABC`
        # directly and has `abc.abstractmethod` decorators). See: https://github.com/python/mypy/issues/4717
        type_args = t.get_args(getattr(self, "__orig_class__", None))
        assert len(type_args) == 1

        type_t = t.get_origin(type_args[0]) or type_args[0]
        assert isinstance(type_t, type)

        return type_t

    def __load(self, entrypoint: str) -> object:
        match entrypoint.split(":", maxsplit=1):
            case [module, attr]:
                try:
                    loaded_module = importlib.import_module(module)

                except ImportError as err:
                    details = "invalid entrypoint module"
                    raise ValueError(details, entrypoint) from err

                try:
                    obj: object = getattr(loaded_module, attr)

                except AttributeError as err:
                    details = "invalid entrypoint attribute"
                    raise ValueError(details, entrypoint) from err

                return obj

            case _:
                details = "invalid entrypoint"
                raise ValueError(details, entrypoint)

    def __check_class(self, obj: object) -> t.TypeGuard[type[T]]:
        return isinstance(obj, type) and issubclass(obj, self.__type)

    def __check_instance(self, obj: object) -> t.TypeGuard[T]:
        return isinstance(obj, self.__type)
