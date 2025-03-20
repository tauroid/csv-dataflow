from typing import Iterator, TypeVar
from csv_dataflow.sop import SumProductNode, SumProductPath

T = TypeVar("T")
Data = TypeVar("Data", default=None)


def iterate(
    sop: SumProductNode[T, Data], prefix: SumProductPath[T] = ()
) -> Iterator[SumProductPath[T]]:
    if not sop.children:
        yield prefix
    else:
        for child_path, child in sop.children.items():
            assert not isinstance(
                child, int
            ), "Flat iterating over a recursive type is probably a mistake"
            for path in iterate(child, (*prefix, child_path)):
                yield path
