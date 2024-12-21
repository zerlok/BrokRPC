import typing as t

from pydantic import BaseModel


class FooModel(BaseModel):
    class Bar(BaseModel):
        str2int: t.Mapping[str, int]

    num: int
    s: str
    bar: Bar
