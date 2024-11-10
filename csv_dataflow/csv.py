import csv
from frozendict import frozendict
from itertools import chain
from pathlib import Path
from typing import Any, Iterator, TypeVar

from .cons import Cons, ConsList, at_index
from .relation import ParallelRelation
from .sop import SumProductNode, SumProductPath, sop_from_type

S = TypeVar("S")
T = TypeVar("T")


# FIXME make this take all column names and produce a tree
def select_given_csv_column_names(
    sop: SumProductNode[T],
    name_paths: tuple[tuple[str, ...], ...],
    immediate_name_paths: tuple[tuple[str, ...], ...] = (),
    prev_stack: ConsList[SumProductNode[T]] = None,
    recurse: bool = True,
) -> SumProductNode[T]:
    if () in immediate_name_paths:
        # We matched to the end of a name_path
        return sop

    stack = Cons(sop, prev_stack)

    def immediate_name_paths_for(child_path: str) -> tuple[tuple[str, ...], ...]:
        return (
            *(tuple(tail) for head, *tail in name_paths if head == child_path),
            *(
                tuple(tail)
                for head, *tail in immediate_name_paths
                if head == child_path
            ),
        )

    # Get results for all children
    filtered_children = {
        path: select_given_csv_column_names(
            child if not isinstance(child, int) else at_index(stack, child),
            name_paths,
            immediate_name_paths_for(path),
            stack,
            # Once we've recursed, don't do it again as we'll
            # uncover all we need to in the first traversal
            # FIXME actually this is only unless the name path
            #       can only match 2+ loops through. so maybe
            #       allow immediate paths to force recursion
            recurse and not isinstance(child, int)
        )
        for path, child in sop.children.items()
        if not isinstance(child, int) or recurse
    }

    # FIXME get rid of children with no children that don't match here

    for path, child in sop.children.items():
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
