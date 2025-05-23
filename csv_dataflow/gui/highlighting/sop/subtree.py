from typing import Any, Iterable

from frozendict import frozendict
from csv_dataflow.gui.highlighting.sop.context import (
    HighlightingContext,
)
from csv_dataflow.sop import (
    SOPPathElement,
    SumProductChild,
    SumProductNode,
)


def subtree_in_relation[S, T](
    context: HighlightingContext[S, T],
    sop: SumProductNode[Any],
    children_relation_status_iter: Iterable[
        tuple[SOPPathElement, SumProductNode[Any, bool]]
    ],
) -> SumProductNode[Any, bool]:
    children_relation_status = tuple(
        children_relation_status_iter
    )
    return SumProductNode[Any, bool](
        sop.sop,
        frozendict[SOPPathElement, SumProductChild[bool]](
            children_relation_status
        ),
        (
            context.relations_from_root
            and len(context.relations_from_root.head) > 0
        )
        or any(
            child.data for _, child in children_relation_status
        ),
    )
