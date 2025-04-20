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
    RelationPrefix,
    SeriesRelation,
)
from csv_dataflow.relation.triple import (
    BasicTriple,
    CopyTriple,
    ParallelTriple,
    SeriesTriple,
    Triple,
)
from csv_dataflow.sop import (
    DeBruijn,
    SumProductNode,
    SumProductPath,
)


def highlight_leaf_triple_point(
    point: Literal["Source", "Target"],
    sop: SumProductNode[Any, bool],
    triple: (
        BasicTriple[Any, Any, bool] | CopyTriple[Any, Any, bool]
    ),
    sop_prefix: SumProductPath[Any],
    relation_prefix: RelationPrefix,
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
        case "Target":
            triple_point = triple.target

    highlight_mappings: list[
        dict[RelationPath[Any, Any], set[Highlighting]]
    ] = []
    for sop_path in sop.iter_leaf_paths(sop_prefix):
        path = RelationPath[Any, Any](
            point, sop_path, relation_prefix
        )
        highlight_mappings.append({path: {highlight}})
        highlight_mappings.append(
            {
                RelationPath(
                    point,
                    subpath,
                    relation_prefix,
                ): {sub_highlight}
                for subpath in triple_point.at(
                    path.subtract_prefixes(
                        source_prefix=sop_path,
                        target_prefix=sop_path,
                    ).sop_path
                ).iter_all_paths(sop_path)
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
            highlight_leaf_triple_point(
                "Source",
                relation.source,
                triple,
                triple.source_prefix,
                triple.relation_prefix,
            )
        )

    if relation.target:
        highlight_mappings.append(
            highlight_leaf_triple_point(
                "Target",
                relation.target,
                triple,
                triple.target_prefix,
                triple.relation_prefix,
            )
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
