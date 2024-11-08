import csv
from itertools import chain
from pathlib import Path
from typing import Any, Iterator, TypeVar

from .cons import Cons, ConsList, at_index
from .relation import ParallelRelation
from .sop import SumProductNode, SumProductPath, sop_from_type

S = TypeVar("S")
T = TypeVar("T")


# FIXME make this take all column names and produce a tree
def filter_by_csv_column_names(
    sop: SumProductNode[T],
    name_paths: tuple[str, ...],
    prefix: SumProductPath[Any] = (),
    immediate: bool = False,
    prev_stack: ConsList[SumProductNode[T]] = None,
    recurse: bool = True,
) -> SumProductNode[T]]:
    stack = Cons(sop, prev_stack)
    name = name_path[0]
    something_matched = False
    for key, child in sop.children.items():
        new_prefix = (*prefix, key)
        if key == name:  # FIXME allow the docstring heading too
            something_matched = True
            if len(name_path) == 1:
                yield new_prefix
            else:
                unrolled_child = (
                    at_index(stack, child) if isinstance(child, int) else child
                )
                for path in paths_from_csv_column_name(
                    unrolled_child, name_path[1:], new_prefix, True, stack
                ):
                    yield path
        else:
            if isinstance(child, int):
                continue

            if not immediate:
                for path in paths_from_csv_column_name(child, name_path, new_prefix):
                    yield path

    if immediate and not something_matched:
        raise ValueError(
            f"{name_path} didn't match any immediate child {sop.children.keys()}"
        )


class Source: ...


class Target: ...


End = Source | Target


def parallel_relation_from_csv(
    s: type[S], t: type[T], csv_path: Path
) -> tuple[SumProductNode[S], SumProductNode[T], ParallelRelation[S, T]]:
    sop_s = sop_from_type(s)
    sop_t = sop_from_type(t)

    with open(csv_path) as f:
        csv_dict = csv.DictReader(f)

        relation_paths: list[
            tuple[list[SumProductPath[S]], list[SumProductPath[T]]]
        ] = []

        for row in csv_dict:
            source_paths: list[SumProductPath[S]] = []
            target_paths: list[SumProductPath[T]] = []

            end: End = Source()
            for name, value in row.items():
                # Blank is the separator for now
                if name.strip() == "":
                    end = Target()
                    continue

                # Blank value (for now) means no connection
                if value == "":
                    continue

                match end:
                    case Source():
                        for path in paths_from_csv_column_name(
                            sop_s, tuple(map(str.strip, name.split("/")))
                        ):
                            source_paths.append((*path, value))
                    case Target():
                        for path in paths_from_csv_column_name(
                            sop_t, tuple(map(str.strip, name.split("/")))
                        ):
                            target_paths.append((*path, value))

            relation_paths.append((source_paths, target_paths))

    all_source_paths = chain.from_iterable(p[0] for p in relation_paths)
    all_target_paths = chain.from_iterable(p[1] for p in relation_paths)

    sop_s = add_paths(sop_s, all_source_paths)
    sop_t = add_paths(sop_t, all_target_paths)

    return (
        sop_s,
        sop_t,
        ParallelRelation(
            tuple(
                (
                    BasicRelation(
                        select_from_paths(sop_s, source_paths),
                        select_from_paths(sop_t, target_paths),
                    ),
                    Between((), ()),
                )
                for source_paths, target_paths in relation_paths
            )
        ),
    )
