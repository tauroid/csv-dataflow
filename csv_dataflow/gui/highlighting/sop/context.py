from dataclasses import dataclass, replace
from typing import Collection

from csv_dataflow.cons import (
    Cons,
    ConsList,
    iter_cons_list,
    to_cons_list,
)
from csv_dataflow.relation import RelationPath
from csv_dataflow.relation.filtering import filter_relation
from csv_dataflow.relation.iterators import iter_basic_triples
from csv_dataflow.relation.triple import (
    BasicTriple,
    CopyTriple,
    Triple,
)
from csv_dataflow.sop import SOPPathElement, SumProductNode


@dataclass(frozen=True)
class BasicContext[S, T]:
    relation: BasicTriple[S, T, bool]


@dataclass(frozen=True)
class CopyContext[S, T]:
    relation: CopyTriple[S, T, bool]
    subpath: ConsList[SOPPathElement]


@dataclass(frozen=True)
class HighlightingContext[S, T]:
    path: RelationPath[S, T]
    triple_filtered_to_node: Triple[S, T, bool]
    """bool is whether it's still full"""
    relations_from_root: ConsList[
        Collection[BasicContext[S, T] | CopyContext[S, T]]
    ] = None


def get_relation_context_at_node[S, T](
    path: RelationPath[S, T],
    triple_filtered_to_node: Triple[S, T, bool],
) -> Collection[BasicContext[S, T] | CopyContext[S, T]]:
    node_relation_context: Collection[
        BasicContext[S, T] | CopyContext[S, T]
    ] = []
    for triple in iter_basic_triples(triple_filtered_to_node):
        match path.point:
            case "Source":
                related = triple.relation.source
                prefix = triple.source_prefix
            case "Target":
                related = triple.relation.target
                prefix = triple.target_prefix
            case _:
                assert False

        if related and (
            path.sop_path in related.iter_leaf_paths(prefix)
        ):
            match triple:
                case BasicTriple():
                    node_relation_context.append(
                        BasicContext(triple)
                    )
                case CopyTriple():
                    node_relation_context.append(
                        CopyContext(triple, None)
                    )

    return tuple(node_relation_context)


def extend_subpath[S, T](
    parent_context: BasicContext[S, T] | CopyContext[S, T],
    key: SOPPathElement,
) -> BasicContext[S, T] | CopyContext[S, T]:
    match parent_context:
        case BasicContext():
            return parent_context
        case CopyContext(triple, subpath):
            return CopyContext(triple, Cons(key, subpath))


def refine_related_parent_info[S, T](
    context: HighlightingContext[S, T], key: SOPPathElement
) -> ConsList[
    Collection[BasicContext[S, T] | CopyContext[S, T]]
]:
    """
    `context` has `path` and `triple_filtered_to_node` refined,
    but `related_parent_info` still unrefined
    """
    related_parent_info = Cons(
        get_relation_context_at_node(
            context.path, context.triple_filtered_to_node
        ),
        to_cons_list(
            tuple(
                extend_subpath(relation_context, key)
                for relation_context in relations
            )
            for relations in iter_cons_list(
                context.relations_from_root
            )
        ),
    )

    return related_parent_info


def refine_highlighting_context[S, T, N](
    context: HighlightingContext[S, T],
    key: SOPPathElement,
    _: SumProductNode[N],
) -> HighlightingContext[S, T]:
    """
    If we go into a related path, set related_parent_info to it

    Add key to path, filter relation more
    """
    path = replace(
        context.path, sop_path=context.path.sop_path + (key,)
    )

    triple_filtered_to_node = replace(
        context.triple_filtered_to_node,
        relation=filter_relation(
            context.triple_filtered_to_node.relation, (path,)
        ),
    )

    related_parent_info = refine_related_parent_info(
        replace(
            context,
            path=path,
            triple_filtered_to_node=triple_filtered_to_node,
        ),
        key,
    )

    return HighlightingContext(
        path, triple_filtered_to_node, related_parent_info
    )
