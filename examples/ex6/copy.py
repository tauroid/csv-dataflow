from typing import TypeVar
from csv_dataflow.decompose import decompose

T = TypeVar("T")


@decompose
def copy(x: T) -> tuple[T, T]:
    return (x, x)
