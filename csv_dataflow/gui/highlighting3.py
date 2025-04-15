from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Collection, Iterable, Literal

from csv_dataflow.relation import (
    BasicRelation,
    Copy,
    Relation,
    RelationPath,
)
from csv_dataflow.sop import SumProductNode, SumProductPath


@dataclass(frozen=True)
class BasicContext[S, T]:
    relation: BasicRelation[S, T]


@dataclass(frozen=True)
class CopyContext[S, T]:
    relation: Copy[S, T]
    subpath: SumProductPath[Any]


Highlighting = Literal[
    "BareSelected",
    "Related",
    "RelatedSelected",
    "SubRelated",
    "SubRelatedSelected",
    "HasRelatedChildren",
    "Copy",
    "ChildIsCopySelected",
    "CopySelected",
    "SubCopy",
    "SubCopySelected",
    "SubSubCopy"
]


# Can I have some kind of generic traversal function that takes
# - Context data refiner function
# - Child data combiner function
# - Evaluator function taking results of both of the above
# - Child value combiner function to get final result


def traverse[N, K, Ct, Cd, I, R](
    yield_children: Callable[[N], Iterable[tuple[K, N]]],
    refine_context: Callable[[Ct, K, N], Ct],
    combine_child_data: Callable[
        [N, Iterable[tuple[K, Cd]]], Cd
    ],
    evaluate: Callable[[Ct, Cd], I],
    result: Callable[[I, Iterable[tuple[K, R]]], R],
    context: Ct,
    node: N,
) -> tuple[Cd, R]:
    child_results = tuple(
        (
            key,
            traverse(
                yield_children,
                refine_context,
                combine_child_data,
                evaluate,
                result,
                refine_context(context, key, child),
                child,
            ),
        )
        for key, child in yield_children(node)
    )
    subtree_summary = combine_child_data(
        node,
        (
            (key, child_subtree_summary)
            for key, (child_subtree_summary, _) in child_results
        ),
    )
    node_result = evaluate(context, subtree_summary)
    return subtree_summary, result(
        node_result,
        (
            (key, child_final_result)
            for key, (_, child_final_result) in child_results
        ),
    )


@dataclass(frozen=True)
class HighlightingContext[S, T]:
    prefix: SumProductPath[Any]
    related_parent_info: (
        BasicContext[S, T] | CopyContext[S, T] | None
    )
    relation_filtered_to_node_children: Relation[S, T]


def refine_highlighting_context[S, T, N](
    context: HighlightingContext[S, T], key: str, node: SumProductNode[N]
) -> HighlightingContext[S, T]:
    """
    If we go into a related prefix, set related_parent_info to it

    Add key to prefix, filter relation more
    """


# Doesn't do traversal of sop, takes results of traversal and gives highlighted paths
def node_highlighting[S, T, N](
    context: HighlightingContext[S, T],
    has_related_children: SumProductNode[N, bool],
) -> Collection[tuple[RelationPath[S, T], Highlighting]]:
    match context.related_parent_info:
        case None:
            """
            Do this one BareSelected or RelatedSelected then
            related children then the filtered relation
            """
        case BasicContext(basic_relation):
            """
            This one is SubRelatedSelected then highlight
            basic_relation
            """
        case CopyContext(copy_relation, subpath):
            """
            This one is SubCopySelected, every sop path that's
            `subpath` from a path in copy_relation is
            SubCopy, every child of those is SubSubCopy,
            every path explicitly in copy_relation is Copy,
            everything along `subpath` (anywhere) between a
            copy_relation path and a SubCopy or SubCopySelected
            is ChildIsCopySelected

            Also the relation itself (as in the arrow) is Copy
            """
