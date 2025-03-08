from typing import TypeVar
from csv_dataflow.relation import (
    BasicRelation,
    Between,
    DeBruijn,
    ParallelRelation,
    Relation,
    SeriesRelation,
)

S = TypeVar("S")
T = TypeVar("T")


# NOTE is this too limiting? what functions would violate this?
def assert_subpaths_if_recursive(
    child: Relation[S, T] | DeBruijn, between: Between[S, T]
) -> bool:
    if isinstance(child, int):
        assert between.source and between.target, (
            "Recursion in relations should happen between subpaths"
            " of the source and target"
        )
    return True


def only_has_de_bruijn_indices(relation: Relation[S, T]) -> bool:
    match relation:
        case BasicRelation():
            return False
        case ParallelRelation(children):
            return all(
                isinstance(child, DeBruijn) or only_has_de_bruijn_indices(child)
                for child, _ in children
            )
        case SeriesRelation():
            raise NotImplementedError


def max_de_bruijn_index_relative_to_current_node(relation: Relation[S, T]) -> int:
    """0 is the argument, 1 is node above, etc"""

    match relation:
        case BasicRelation():
            return 0
        case ParallelRelation(children):
            return max(
                (
                    child
                    if isinstance(child, DeBruijn)
                    else max_de_bruijn_index_relative_to_current_node(child) - 1
                )
                for child, _ in children
            )
        case SeriesRelation():
            raise NotImplementedError


def empty_recursion(relation: Relation[S, T]) -> bool:
    return (
        only_has_de_bruijn_indices(relation)
        and max_de_bruijn_index_relative_to_current_node(relation) <= 0
    )
