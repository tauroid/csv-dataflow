from typing import Iterator, TypeVar
from csv_dataflow.sop import (
    DeBruijn,
    SumProductNode,
    SumProductPath,
)

T = TypeVar("T")
Data = TypeVar("Data", default=None)


def iterate_leaves(
    sop: SumProductNode[T, Data], prefix: SumProductPath[T] = ()
) -> Iterator[SumProductPath[T]]:
    if not sop.children:
        yield prefix
    else:
        for child_path, child in sop.children.items():
            assert not isinstance(
                child, int
            ), "Flat iterating over a recursive type is probably a mistake"
            for path in iterate_leaves(
                child, (*prefix, child_path)
            ):
                yield path


def iterate_every(
    sop: SumProductNode[T, Data] | DeBruijn,
    prefix: SumProductPath[T] = (),
) -> Iterator[SumProductPath[T]]:
    for path, _ in iterate_every_with_data(sop, prefix):
        yield path


def iterate_every_with_data(
    sop: SumProductNode[T, Data] | DeBruijn,
    prefix: SumProductPath[T] = (),
) -> Iterator[tuple[SumProductPath[T], Data]]:
    """
    Iterates over every path, not just leaves

    Starts with the root () and works downwards

    Does not include recursed paths
    (come back and add a flag if you want that)
    """
    if isinstance(sop, DeBruijn):
        return

    yield prefix, sop.data

    level = len(prefix) + 1

    child_iterators = tuple(
        iterate_every_with_data(child, (*prefix, child_path))
        for child_path, child in sop.children.items()
    )
    next_paths = tuple(next(it) for it in child_iterators)

    while child_iterators:
        new_child_iterators: list[
            Iterator[tuple[SumProductPath[T], Data]]
        ] = []
        new_next_paths: list[tuple[SumProductPath[T], Data]] = []
        for next_path, child_iterator in zip(
            next_paths, child_iterators
        ):
            yield next_path
            for path in child_iterator:
                if len(path) > level:
                    new_child_iterators.append(child_iterator)
                    new_next_paths.append(path)
                    break

                yield path

        child_iterators = tuple(new_child_iterators)
        next_paths = tuple(new_next_paths)
        level += 1
