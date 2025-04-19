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
    children_relation_status: Iterable[
        tuple[SOPPathElement, SumProductNode[Any, bool]]
    ],
) -> SumProductNode[Any, bool]:
    return SumProductNode[Any, bool](
        sop.sop,
        frozendict[SOPPathElement, SumProductChild[bool]](
            children_relation_status
        ),
        (
            context.related_parent_info
            and len(context.related_parent_info.head) > 0
        )
        or any(
            child.data for _, child in children_relation_status
        ),
    )
