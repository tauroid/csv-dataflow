import csv
from dataclasses import replace
from frozendict import frozendict
from itertools import chain, repeat
from pathlib import Path
from typing import Any, Iterator, TypeVar

from .cons import Cons, ConsList, at_index
from .relation import BasicRelation, Between, ParallelRelation
from .sop import (
    SumProductChild,
    SumProductNode,
    SumProductPath,
    add_paths,
    merge_sops,
    sop_from_type,
)

S = TypeVar("S")
T = TypeVar("T")


def only_has_de_bruijn_indices(sop: SumProductNode[T]) -> bool:
    if not sop.children:
        # Terminal node is data
        return False

    return all(
        isinstance(child, int) or only_has_de_bruijn_indices(child)
        for child in sop.children.values()
    )


def max_de_bruijn_index_relative_to_current_node(sop: SumProductNode[T]) -> int:
    """0 is the argument, 1 is node above, etc"""

    if not sop.children:
        # This suits if the purpose is just to tell if the node doesn't
        # refer outside itself
        return 0

    return max(
        (
            child
            if isinstance(child, int)
            else max_de_bruijn_index_relative_to_current_node(child) - 1
        )
        for child in sop.children.values()
    )


def empty_recursion(sop: SumProductNode[T]) -> bool:
    return (
        only_has_de_bruijn_indices(sop)
        and max_de_bruijn_index_relative_to_current_node(sop) == 0
    )


def select_given_csv_paths(
    sop: SumProductNode[T],
    name_paths: tuple[tuple[str, ...], ...],
    immediate_name_paths: tuple[tuple[str, ...], ...] = (),
    prev_stack: ConsList[SumProductNode[T]] = None,
) -> SumProductNode[T]:
    """
    Will also include open de Bruijn indices as they could still
    be put in a context that matches
    """
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

    child_immediate_name_paths = {
        path: immediate_name_paths_for(path) for path in sop.children
    }

    # Apply this function to all children
    filtered_children = {
        path: filtered_child
        for path, child in sop.children.items()
        for filtered_child in (
            (
                (
                    select_given_csv_paths(
                        child if not isinstance(child, int) else at_index(stack, child),
                        # No new matches in the mirror realm
                        name_paths if not isinstance(child, int) else (),
                        child_immediate_name_paths[path],
                        stack,
                    )
                )
                if not isinstance(child, int) or child_immediate_name_paths[path]
                else child
            ),
        )
    }

    # Filter down to children that:
    #   - Have real data in their (filtered) children, or
    #   - Are selected by a path, or
    #   - Have (or are) de Bruijn indices that remain open above
    #     the current node's level
    selected_children_with_data_or_open_de_bruijn_indices = {
        path: child
        for path, child in filtered_children.items()
        if (
            not isinstance(child, int)
            and (
                (
                    child.children
                    and (
                        not only_has_de_bruijn_indices(child)
                        or max_de_bruijn_index_relative_to_current_node(child) > 1
                    )
                )
                or (not child.children and (path,) in name_paths)
            )
        )
        or (isinstance(child, int) and child > 0)
    }

    return replace(
        sop,
        children=frozendict[str, SumProductChild](
            {
                **selected_children_with_data_or_open_de_bruijn_indices,
                # de Bruijn indices directly referencing this node, only
                # if this node has other children with actual data / open indices
                **(
                    {
                        path: child
                        for path, child in filtered_children.items()
                        if (isinstance(child, int) and child == 0)
                        or (
                            not isinstance(child, int)
                            and only_has_de_bruijn_indices(child)
                            and max_de_bruijn_index_relative_to_current_node(child) == 1
                        )
                    }
                    if len(selected_children_with_data_or_open_de_bruijn_indices) > 0
                    else {}
                ),
            }
        ),
    )


class Source: ...


class Target: ...


End = Source | Target


def csv_name_to_name_path(csv_name: str) -> tuple[str, ...]:
    return tuple(map(str.strip, csv_name.split("/")))


def parallel_relation_from_csv(
    s: type[S], t: type[T], csv_path: Path
) -> tuple[SumProductNode[S], SumProductNode[T], ParallelRelation[S, T]]:
    sop_s = sop_from_type(s)
    sop_t = sop_from_type(t)

    relations: list[BasicRelation[S, T]] = []

    all_source_value_paths: list[tuple[str, ...]] = []
    all_target_value_paths: list[tuple[str, ...]] = []

    with open(csv_path) as f:
        csv_dict = csv.DictReader(f)

        for row in csv_dict:
            source_value_paths: list[tuple[str, ...]] = []
            target_value_paths: list[tuple[str, ...]] = []

            end: End = Source()
            for name, value in row.items():
                # Blank is the separator for now
                if name.strip() == "":
                    end = Target()
                    continue

                # Blank value (for now) means no connection
                if value == "":
                    continue

                value_path = (*csv_name_to_name_path(name), value)

                match end:
                    case Source():
                        source_value_paths.append(value_path)
                        all_source_value_paths.append(value_path)
                    case Target():
                        target_value_paths.append(value_path)
                        all_target_value_paths.append(value_path)

            source_value_paths_tuple = tuple(source_value_paths)
            target_value_paths_tuple = tuple(target_value_paths)

            relations.append(
                BasicRelation(
                    select_given_csv_paths(
                        add_paths(sop_s, source_value_paths_tuple),
                        source_value_paths_tuple,
                    ),
                    select_given_csv_paths(
                        add_paths(sop_t, target_value_paths_tuple),
                        target_value_paths_tuple,
                    ),
                )
            )

    return (
        add_paths(sop_s, tuple(all_source_value_paths)),
        add_paths(sop_t, tuple(all_target_value_paths)),
        ParallelRelation(tuple(zip(relations, repeat(Between[S, T]((), ()))))),
    )
