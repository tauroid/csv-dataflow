from typing import Any, Iterable
from csv_dataflow.gui.highlighting.sop.context import (
    refine_highlighting_context,
)
from csv_dataflow.gui.highlighting.sop.subtree import (
    subtree_in_relation,
)
from csv_dataflow.gui.highlighting.sop.traverse import traverse
from csv_dataflow.sop import (
    DeBruijn,
    SOPPathElement,
    SumProductNode,
)


def sop_div(sop: SumProductNode[Any]) -> str:
    _, div = _sop_div_with_intermediate(sop)
    return div


def sop_children(
    sop: SumProductNode[Any],
) -> Iterable[tuple[SOPPathElement, SumProductNode[Any]]]:
    for name, child in sop.children.items():
        assert not isinstance(child, DeBruijn)
        yield name, child


def _sop_div_with_intermediate[T](
    sop: SumProductNode[T],
) -> tuple[SumProductNode[T, bool], str]:
    return traverse(
        sop_children,
        refine_highlighting_context,
        subtree_in_relation,
    )
