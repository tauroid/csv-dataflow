import csv
from dataclasses import replace
from frozendict import frozendict
from itertools import repeat
from pathlib import Path

from csv_dataflow.relation.triple import ParallelTriple, Triple
from csv_dataflow.sop.from_type import sop_from_type

from .cons import Cons, ConsList, at_index
from .relation import (
    BasicRelation,
    Between,
    ParallelRelation,
)
from .sop import (
    SumProductChild,
    SumProductNode,
)


def select_given_csv_paths[T](
    sop: SumProductNode[T],
    name_paths: tuple[tuple[str, ...], ...],
    immediate_name_paths: tuple[tuple[str, ...], ...] = (),
    prev_stack: ConsList[SumProductNode[T]] = None,
) -> SumProductNode[T] | None:
    """
    Will also include open de Bruijn indices as they could still
    be put in a context that matches
    """
    if () in immediate_name_paths:
        # We matched to the end of a name_path
        return sop

    stack = Cons(sop, prev_stack)

    def immediate_name_paths_for(
        child_path: str,
    ) -> tuple[tuple[str, ...], ...]:
        return (
            *(
                tuple(tail)
                for head, *tail in name_paths
                if head == child_path
            ),
            *(
                tuple(tail)
                for head, *tail in immediate_name_paths
                if head == child_path
            ),
        )

    child_immediate_name_paths = {
        path: immediate_name_paths_for(path)
        for path in sop.children
    }

    # The filtered children have either one or more given paths, or
    # open de Bruijn indices, or both
    # Children not containing any selected paths return None (again,
    # unless they have open de Bruijn indices)
    filtered_sop = replace(
        sop,
        children=frozendict[str, SumProductChild](
            {
                path: filtered_child
                for path, child in sop.children.items()
                for filtered_child in (
                    (
                        (
                            select_given_csv_paths(
                                (
                                    child
                                    if not isinstance(child, int)
                                    else at_index(stack, child)
                                ),
                                # No new matches in the mirror realm
                                (
                                    name_paths
                                    if not isinstance(child, int)
                                    else ()
                                ),
                                child_immediate_name_paths[path],
                                stack,
                            )
                        )
                        if not isinstance(child, int)
                        or child_immediate_name_paths[path]
                        else child
                    ),
                )
                if filtered_child is not None
            }
        ),
    )

    if (
        not filtered_sop.children
        or filtered_sop.is_empty_recursion()
    ):
        return None

    return filtered_sop


class Source: ...


class Target: ...


End = Source | Target


def csv_name_to_name_path(csv_name: str) -> tuple[str, ...]:
    return tuple(map(str.strip, csv_name.split("/")))


def parallel_relation_from_csv[S, T](
    s: type[S], t: type[T], csv_path: Path
) -> Triple[S, T]:
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

                value_path = (
                    *csv_name_to_name_path(name),
                    value,
                )

                match end:
                    case Source():
                        source_value_paths.append(value_path)
                        all_source_value_paths.append(value_path)
                    case Target():
                        target_value_paths.append(value_path)
                        all_target_value_paths.append(value_path)

            source_value_paths_tuple = tuple(source_value_paths)
            target_value_paths_tuple = tuple(target_value_paths)

            sop_s_with_values = sop_s.add_values_at_paths(
                source_value_paths_tuple
            )
            assert not isinstance(sop_s_with_values, int)
            sop_t_with_values = sop_t.add_values_at_paths(
                target_value_paths_tuple
            )
            assert not isinstance(sop_t_with_values, int)

            source = select_given_csv_paths(
                sop_s_with_values,
                source_value_paths_tuple,
            )
            assert source is not None
            target = select_given_csv_paths(
                sop_t_with_values,
                target_value_paths_tuple,
            )
            assert target is not None

            relations.append(BasicRelation(source, target))

    sop_s_with_all_values = sop_s.add_values_at_paths(
        tuple(all_source_value_paths)
    )
    assert not isinstance(sop_s_with_all_values, int)
    sop_t_with_all_values = sop_t.add_values_at_paths(
        tuple(all_target_value_paths)
    )
    assert not isinstance(sop_t_with_all_values, int)

    return ParallelTriple(
        ParallelRelation(
            tuple(zip(relations, repeat(Between[S, T]((), ()))))
        ),
        source=sop_s_with_all_values,
        target=sop_t_with_all_values,
    )
