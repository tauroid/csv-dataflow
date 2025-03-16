from itertools import accumulate, chain, repeat
from typing import TypeVar

from csv_dataflow.sop import SumProductPath
from ..relation import BasicRelation, ParallelChildIndex, ParallelRelation, Relation, RelationPathElement, SeriesRelation
from ..relation.iterators import iter_relation_paths

from .asserts import assert_isinstance, assert_true

S = TypeVar("S")
T = TypeVar("T")

def relation_id_from_path(path: tuple[RelationPathElement, ...]) -> str:
    return f"rel_:{":".join(map(str, path))}"


def relation_ids_to_highlight(
    filtered_relation: Relation[S, T],
    full_relation: Relation[S, T] | None = None,
    prefix: tuple[RelationPathElement, ...] = (),
) -> tuple[str, ...] | None:
    """
    None means no discrepancy yet between filtered_relation and full_relation

    So if the top returns None, highlight the top relation's id
    """
    match filtered_relation:
        case BasicRelation(source, target):
            if source is None and target is None:
                return ()

            if full_relation is None:
                return (relation_id_from_path(prefix),)

            # Both are populated and we assume from the structural
            # recursion and how filtering works on BasicRelations
            # that they are the same
            assert filtered_relation == full_relation
            return None
        case ParallelRelation(children):
            assert isinstance(full_relation, ParallelRelation) or full_relation is None
            child_relation_ids = tuple(
                relation_ids_to_highlight(
                    filtered_child, full_child, (*prefix, ParallelChildIndex(i))
                )
                for i, (
                    (filtered_child, filtered_between),
                    (full_child, full_between),
                ) in enumerate(
                    zip(
                        children,
                        (
                            full_relation.children
                            if full_relation is not None
                            else repeat((None, None))
                        ),
                    )
                )
                if assert_true(full_between is None or filtered_between == full_between)
                and not assert_isinstance(filtered_child, int, False)
                and not assert_isinstance(full_child, int, False)
            )
            if all(ids is None for ids in child_relation_ids):
                return None
            else:
                return tuple(
                    chain.from_iterable(
                        (
                            ids
                            if ids is not None
                            else (
                                relation_id_from_path((*prefix, ParallelChildIndex(i))),
                            )
                        )
                        for i, ids in enumerate(child_relation_ids)
                    )
                )
        case SeriesRelation():
            raise NotImplementedError


def highlight_related_on_hover(
    filtered_relation: Relation[S, T],
    full_relation: Relation[S, T] | None = None,
    relation_prefix: tuple[RelationPathElement, ...] = (),
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
) -> str:
    relation_paths = filter(
        lambda p: len(p) > 1,
        chain.from_iterable(
            accumulate(map(lambda x: (x,), path.flat()))
            for path in iter_relation_paths(
                filtered_relation, source_prefix, target_prefix
            )
        ),
    )
    related_ids = (f"#{":".join(map(str, path))}" for path in set(relation_paths))
    relation_ids = relation_ids_to_highlight(
        filtered_relation, full_relation, relation_prefix
    )
    if relation_ids is not None:
        relation_ids = map(lambda relation_id: f"#{relation_id}", relation_ids)
    else:
        relation_ids = (f"#{relation_id_from_path(relation_prefix)}",)
    return (
        f"on mouseover halt the event's bubbling toggle .highlighted on [{",".join(chain(related_ids, relation_ids))}] until mouseout"
        if related_ids
        else ""
    )
