from __future__ import annotations

import typing as t
from functools import singledispatch, wraps

if t.TYPE_CHECKING:
    from types import TracebackType


class ErrorTransformer(t.ContextManager[None], t.AsyncContextManager[None]):
    def __init__(self) -> None:
        self.__transform = singledispatch(self.__transform_default)

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        self.__handle(exc_value)

    async def __aenter__(self) -> None:
        pass

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        self.__handle(exc_value)

    def register[U: BaseException](
        self,
        func: t.Callable[[U], BaseException | None],
    ) -> t.Callable[[U], BaseException | None]:
        self.__transform.register(func)
        return func

    def wrap[**U, V](
        self,
        func: t.Callable[U, t.Coroutine[t.Any, t.Any, V]],
    ) -> t.Callable[U, t.Coroutine[t.Any, t.Any, V]]:
        @wraps(func)
        async def wrapper(*args: U.args, **kwargs: U.kwargs) -> V:
            with self:
                return await func(*args, **kwargs)

        return wrapper

    def __transform_default(self, err: BaseException) -> BaseException | None:
        return err

    def __handle(self, err: BaseException | None) -> None:
        if err is None:
            return

        transformed_err = self.__transform(err)
        if transformed_err is None or transformed_err is err:
            return

        else:
            raise transformed_err from err
