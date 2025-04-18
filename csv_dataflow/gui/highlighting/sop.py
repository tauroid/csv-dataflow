from dataclasses import dataclass, replace
from typing import (
    Any,
    Callable,
    Collection,
    Iterable,
    Mapping,
)

from csv_dataflow.cons import ConsList
from csv_dataflow.gui.highlighting.modes import Highlighting
from csv_dataflow.relation import (
    BasicRelation,
    Copy,
    RelationPath,
    Triple,
)
from csv_dataflow.relation.filtering import filter_relation
from csv_dataflow.relation.iterators import (
    iter_basic_triples,
)
from csv_dataflow.sop import SumProductNode, SumProductPath


@dataclass(frozen=True)
class BasicContext[S, T]:
    # FIXME Make triple versions of all the relations
    #       Then at_parallel_child can be less silly and types can
    #       get more precise in a bunch of places
    relation: BasicTriple[S, T]


@dataclass(frozen=True)
class CopyContext[S, T]:
    relation: CopyTriple[S, T]
    subpath: SumProductPath[Any]


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
    path: RelationPath[S, T]
    triple_filtered_to_node: Triple[S, T, bool]
    """bool is whether it's still full"""
    related_parent_info: ConsList[
        Collection[BasicContext[S, T] | CopyContext[S, T]]
    ] = None


def refine_highlighting_context[S, T, N](
    context: HighlightingContext[S, T],
    key: str,
    node: SumProductNode[N],
) -> HighlightingContext[S, T]:
    """
    If we go into a related path, set related_parent_info to it

    Add key to path, filter relation more
    """
    path = replace(
        context.path, sop_path=(*context.path.sop_path, key)
    )
    triple_filtered_to_node = replace(
        context.triple_filtered_to_node,
        relation=filter_relation(
            context.triple_filtered_to_node.relation, (path,)
        ),
    )
    # If path in relation (explicitly), set related_parent_info
    # to the BasicRelations and/or Copys with the path
    # So basically iterate over the relation and accumulate the
    # ones with the path
    # (need an iterator over (relation, paths at path.point))
    # (or just iter_basic_relations but for triples using Betweens
    # properly) (generally just switch iterator.py to triples)
    related_parent_info: Collection[
        BasicContext[S, T] | CopyContext[S, T]
    ] = []
    for triple in iter_basic_triples(triple_filtered_to_node):
        match triple.relation:
            case BasicRelation() | Copy():
                ...
            case _:
                assert False

        match path.point:
            case "Source":
                related = triple.relation.source
                prefix = triple.source_prefix
            case "Target":
                related = triple.relation.target
                prefix = triple.target_prefix
            case _:
                assert False

        if related and path.sop_path in related.iter_leaf_paths(
            prefix
        ):
            match triple.relation:
                case BasicRelation():
                    related_parent_info.append(
                        BasicContext(triple.relation)
                    )


# Doesn't do traversal of sop, takes results of traversal and gives highlighted paths
def highlighting_from_node_mouseover[S, T, N](
    context: HighlightingContext[S, T],
    has_related_children: SumProductNode[N, bool],
) -> Mapping[RelationPath[S, T], Highlighting]:
    """
    Just highlighting a relation without Selected etc is a
    commonality to this that I should do first
    """

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
