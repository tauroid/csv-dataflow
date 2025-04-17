from typing import Any, Literal, Mapping
from csv_dataflow.gui.highlighting.merge import (
    merge_path_highlights,
)
from csv_dataflow.gui.highlighting.sop import Highlighting
from csv_dataflow.relation import (
    BasicRelation,
    Copy,
    ParallelChildIndex,
    ParallelRelation,
    RelationPath,
    RelationPrefix,
    SeriesRelation,
    Triple,
)
from csv_dataflow.sop import (
    DeBruijn,
    SumProductNode,
    SumProductPath,
)


def highlight_leaf_relation_point(
    point: Literal["Source", "Target"],
    sop: SumProductNode[Any, bool],
    triple: Triple[Any, Any, bool],
    sop_prefix: SumProductPath[Any],
    relation_prefix: RelationPrefix,
) -> Mapping[RelationPath[Any, Any], Highlighting]:
    assert isinstance(
        triple.relation, BasicRelation
    ) or isinstance(triple.relation, Copy)

    match triple.relation:
        case BasicRelation():
            highlight = "Related"
            sub_highlight = "SubRelated"
        case Copy():
            highlight = "Copy"
            sub_highlight = "SubCopy"

    highlight_mappings: list[
        dict[RelationPath[Any, Any], Highlighting]
    ] = []

    for sop_path in sop.iter_leaf_paths(sop_prefix):
        path = RelationPath[Any, Any](
            point, sop_path, relation_prefix
        )
        highlight_mappings.append({path: highlight})
        highlight_mappings.append(
            {
                RelationPath(
                    point,
                    subpath,
                    relation_prefix,
                ): sub_highlight
                for subpath in triple.source.at(
                    path.subtract_prefixes(
                        source_prefix=sop_path
                    ).sop_path
                ).iter_all_paths(sop_path)
            }
        )

    return merge_path_highlights(highlight_mappings)


def highlight_leaf_relation[S, T](
    triple: Triple[S, T, bool], parent_is_full: bool = False
) -> Mapping[RelationPath[S, T], Highlighting]:
    relation = triple.relation

    assert isinstance(relation, BasicRelation) or isinstance(
        relation, Copy
    )

    highlight_mappings: list[
        Mapping[RelationPath[S, T], Highlighting]
    ] = []

    if not parent_is_full:
        match relation:
            case BasicRelation():
                highlight = "Related"
            case Copy():
                highlight = "Copy"

        highlight_mappings.append(
            {
                RelationPath(
                    None, (), triple.relation_prefix
                ): highlight
            }
        )

    if relation.source:
        highlight_mappings.append(
            highlight_leaf_relation_point(
                "Source",
                relation.source,
                triple,
                triple.source_prefix,
                triple.relation_prefix,
            )
        )

    if relation.target:
        highlight_mappings.append(
            highlight_leaf_relation_point(
                "Target",
                relation.target,
                triple,
                triple.target_prefix,
                triple.relation_prefix,
            )
        )

    return merge_path_highlights(highlight_mappings)


def highlight_triple[S, T](
    triple: Triple[S, T, bool],
    parent_is_full: bool = False,
) -> Mapping[RelationPath[S, T], Highlighting]:
    """
    Args:
      relation:
        Relation to highlight. The data is whether the relation
        node's descendants are the full set in the implicit
        "unfiltered" relation, or whether some are missing.
        If some are missing, we can't just light up that node's
        arrow, we have to progress to its children.
    """
    match triple.relation:
        case BasicRelation() | Copy():
            return highlight_leaf_relation(
                triple, parent_is_full
            )
        case ParallelRelation(children, full):
            if not parent_is_full and full:
                """
                Relation path as Related, or Copy if all children
                are Copy (just recurse here to find out)
                """
            if parent_is_full:
                assert full
            return merge_path_highlights(
                (
                    # {
                    #     RelationPath(
                    #         None, (), relation_prefix
                    #     ): this_node_if_newly_full
                    # },
                    highlight_triple(
                        triple.at_parallel_child(
                            ParallelChildIndex(i)
                        )
                    )
                    for i, (child, _) in enumerate(children)
                    if not isinstance(child, DeBruijn)
                )
            )
        case SeriesRelation():
            raise NotImplementedError
