from typing import Mapping
from csv_dataflow.cons import iter_cons_list
from csv_dataflow.gui.highlighting.modes import Highlighting
from csv_dataflow.gui.highlighting.sop.context import (
    HighlightingContext,
)
from csv_dataflow.gui.highlighting.triple import highlight_triple
from csv_dataflow.relation import RelationPath
from csv_dataflow.sop import SumProductNode


def highlighting_from_node_mouseover[S, T, N](
    context: HighlightingContext[S, T],
    subtree_in_relation: SumProductNode[N, bool],
) -> Mapping[RelationPath[S, T], set[Highlighting]]:
    """
    Just highlighting a relation without Selected etc is a
    commonality to this that I should do first
    """

    return highlight_triple(context.triple_filtered_to_node)

    # highlight_mappings: list[
    #     Mapping[RelationPath[S, T], Highlighting]
    # ] = []
    # for node_relations in iter_cons_list(
    #     context.relations_from_root
    # ):
    #     for relation_context in node_relations:

    #     pass

    # match context.relations_from_root:
    #     case None:
    #         """
    #         Do this one BareSelected or RelatedSelected then
    #         relation in subtree then the filtered relation
    #         """
    #     case BasicContext(basic_relation):
    #         """
    #         This one is SubRelatedSelected then highlight
    #         basic_relation
    #         """
    #     case CopyContext(copy_relation, subpath):
    #         """
    #         This one is SubCopySelected, every sop path that's
    #         `subpath` from a path in copy_relation is
    #         SubCopy, every child of those is SubSubCopy,
    #         every path explicitly in copy_relation is Copy,
    #         everything along `subpath` (anywhere) between a
    #         copy_relation path and a SubCopy or SubCopySelected
    #         is ChildIsCopySelected

    #         Also the relation itself (as in the arrow) is Copy
    #         """
