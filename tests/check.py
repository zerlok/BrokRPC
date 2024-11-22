from unittest.mock import create_autospec


def check_instance[T](type_: type[T]) -> T:
    mock = create_autospec(type_)

    def is_eq(other: object) -> bool:
        return isinstance(other, type_)

    mock.__eq__.side_effect = is_eq

    assert isinstance(mock, type_)

    return mock
