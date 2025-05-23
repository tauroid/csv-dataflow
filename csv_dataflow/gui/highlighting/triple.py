from typing import Any, Literal, Mapping
from csv_dataflow.gui.highlighting.merge import (
    merge_path_highlights,
)
from csv_dataflow.gui.highlighting.modes import Highlighting
from csv_dataflow.relation import (
    BasicRelation,
    Copy,
    ParallelChildIndex,
    ParallelRelation,
    Relation,
    RelationPath,
    SeriesRelation,
)
from csv_dataflow.relation.triple import (
    BasicTriple,
    CopyTriple,
    ParallelTriple,
    SeriesTriple,
    Triple,
)
from csv_dataflow.sop import DeBruijn


def highlight_leaf_triple_point(
    point: Literal["Source", "Target"],
    triple: (
        BasicTriple[Any, Any, bool] | CopyTriple[Any, Any, bool]
    ),
) -> Mapping[RelationPath[Any, Any], set[Highlighting]]:
    match triple:
        case BasicTriple():
            highlight = "Related"
            sub_highlight = "SubRelated"
        case CopyTriple():
            highlight = "Copy"
            sub_highlight = "SubCopy"

    match point:
        case "Source":
            triple_point = triple.source
            relation_point = triple.relation.source
            point_prefix = triple.source_prefix
        case "Target":
            triple_point = triple.target
            relation_point = triple.relation.target
            point_prefix = triple.target_prefix

    if not relation_point:
        return {}

    highlight_mappings: list[
        dict[RelationPath[Any, Any], set[Highlighting]]
    ] = []
    for sop_path in relation_point.iter_leaf_paths():
        path = RelationPath[Any, Any](
            point,
            point_prefix + sop_path,
            triple.relation_prefix,
        )
        highlight_mappings.append({path: {highlight}})
        subtree = triple_point.at(sop_path)
        highlight_mappings.append(
            {
                RelationPath(
                    point,
                    point_prefix + subpath,
                    triple.relation_prefix,
                ): {sub_highlight}
                for subpath in subtree.iter_all_paths(sop_path)
            }
        )

    return merge_path_highlights(highlight_mappings)


def highlight_leaf_triple[S, T](
    triple: BasicTriple[S, T, bool] | CopyTriple[S, T, bool],
    parent_is_full: bool = False,
) -> Mapping[RelationPath[S, T], set[Highlighting]]:
    relation = triple.relation

    match relation:
        case BasicRelation():
            highlight = "Related"
        case Copy():
            highlight = "Copy"

    highlight_mappings: list[
        Mapping[RelationPath[S, T], set[Highlighting]]
    ] = []

    if not parent_is_full and relation.data:
        highlight_mappings.append(
            {
                RelationPath(None, (), triple.relation_prefix): {
                    highlight
                }
            }
        )

    if relation.source:
        highlight_mappings.append(
            highlight_leaf_triple_point("Source", triple)
        )

    if relation.target:
        highlight_mappings.append(
            highlight_leaf_triple_point("Target", triple)
        )

    return merge_path_highlights(highlight_mappings)


def is_only_copy[S, T](relation: Relation[S, T, bool]) -> bool:
    match relation:
        case BasicRelation():
            return False
        case Copy():
            return True
        case ParallelRelation(children):
            return all(
                is_only_copy(child)
                for child, _ in children
                if not isinstance(child, DeBruijn)
            )
        case SeriesRelation():
            raise NotImplementedError


def highlight_parallel_triple[S, T](
    triple: ParallelTriple[S, T, bool],
    parent_is_full: bool = False,
) -> Mapping[RelationPath[S, T], set[Highlighting]]:
    full = triple.relation.data

    highlight_mappings: list[
        Mapping[RelationPath[S, T], set[Highlighting]]
    ] = []
    if not parent_is_full and full:
        highlight_mappings.append(
            {
                RelationPath(None, (), triple.relation_prefix): {
                    (
                        "Copy"
                        if is_only_copy(triple.relation)
                        else "Related"
                    )
                }
            }
        )

    if parent_is_full:
        assert full

    highlight_mappings.extend(
        highlight_triple(
            triple.at_child(ParallelChildIndex(i)),
            full,
        )
        for i, (child, _) in enumerate(triple.relation.children)
        if not isinstance(child, DeBruijn)
    )

    return merge_path_highlights(highlight_mappings)


def highlight_triple[S, T](
    triple: Triple[S, T, bool],
    parent_is_full: bool = False,
) -> Mapping[RelationPath[S, T], set[Highlighting]]:
    """
    Args:
      relation:
        Relation to highlight. The data is whether the relation
        node's descendants are the full set in the implicit
        "unfiltered" relation, or whether some are missing.
        If some are missing, we can't just light up that node's
        arrow, we have to progress to its children.
    """
    match triple:
        case BasicTriple() | CopyTriple():
            return highlight_leaf_triple(triple, parent_is_full)
        case ParallelTriple():
            return highlight_parallel_triple(
                triple, parent_is_full
            )
        case SeriesTriple():
            raise NotImplementedError
