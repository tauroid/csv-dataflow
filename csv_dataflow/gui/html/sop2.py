from dataclasses import dataclass
from functools import partial
import inspect
from typing import Any, Iterable, Literal, Mapping

from csv_dataflow.cons import Cons
from csv_dataflow.gui.highlighting.hyperscript import hyperscript
from csv_dataflow.gui.highlighting.modes import Highlighting
from csv_dataflow.gui.highlighting.sop.context import (
    HighlightingContext,
    get_relation_context_at_node,
    refine_highlighting_context,
)
from csv_dataflow.gui.highlighting.sop.node import (
    highlighting_from_node_mouseover,
)
from csv_dataflow.gui.highlighting.sop.subtree import (
    subtree_in_relation,
)
from csv_dataflow.gui.highlighting.sop.traverse import traverse
from csv_dataflow.relation import RelationPath
from csv_dataflow.relation.triple import Triple
from csv_dataflow.sop import (
    DeBruijn,
    SOPPathElement,
    SumProductNode,
)


def sop_html[S, T](
    page_name: str,
    triple: Triple[S, T, bool],
    point: Literal["Source", "Target"],
    sop: SumProductNode[Any],
) -> str:
    _, div = _sop_div_with_intermediate(
        page_name, triple, point, sop
    )
    return div


def sop_children(
    sop: SumProductNode[Any],
) -> Iterable[tuple[SOPPathElement, SumProductNode[Any]]]:
    for name, child in sop.children.items():
        assert not isinstance(child, DeBruijn)
        yield name, child


@dataclass(frozen=True)
class NodeInfo[S, T]:
    sop: Literal["+", "*"]
    path: RelationPath[S, T]
    highlighting: Mapping[RelationPath[S, T], set[Highlighting]]


def node_info[S, T, N](
    context: HighlightingContext[S, T],
    subtree_in_relation: SumProductNode[N, bool],
) -> NodeInfo[S, T]:
    return NodeInfo(
        subtree_in_relation.sop,
        context.path,
        highlighting_from_node_mouseover(
            context, subtree_in_relation
        ),
    )


def _sop_div_from_node_and_child_divs[S, T](
    page_name: str,
    node_info: NodeInfo[S, T],
    child_divs_iter: Iterable[tuple[SOPPathElement, str]],
) -> str:
    expand_path = (
        f"{page_name}/expanded/{node_info.path.as_url_path}"
    )
    child_divs = tuple(child_divs_iter)
    if child_divs:
        method = "delete"
        match node_info.sop:
            case "+":
                sop_class = "sum"
            case "*":
                sop_class = "product"
    else:
        method = "put"
        sop_class = ""

    if node_info.path.sop_path:
        hx_attrs = (
            f'hx-{method}="{expand_path}" hx-swap="outerHTML"'
        )
        if child_divs:
            hx_attrs += f' hx-target="closest .{sop_class}"'
    else:
        hx_attrs = ""

    _ = hyperscript(node_info.highlighting)

    if node_info.path.sop_path:
        label = node_info.path.sop_path[-1]
    else:
        label = node_info.path.point
        assert label

    node_div = inspect.cleandoc(
        f"""
        <div id="{node_info.path.as_id}" _="{_}" {hx_attrs}>
            {label}
        </div>
        """
    )

    if child_divs:
        return inspect.cleandoc(
            f"""
            <div class="{sop_class}">
                {node_div}
                <div>{"".join(div for _,div in child_divs)}</div>
            </div>
            """
        )
    else:
        return node_div


def _sop_div_with_intermediate[S, T](
    page_name: str,
    triple: Triple[S, T, bool],
    point: Literal["Source", "Target"],
    sop: SumProductNode[Any],
) -> tuple[SumProductNode[T, bool], str]:
    path = RelationPath[S, T](point, (), ())
    return traverse(
        sop_children,
        refine_highlighting_context,
        subtree_in_relation,
        node_info,
        partial(_sop_div_from_node_and_child_divs, page_name),
        HighlightingContext(
            path,
            triple,
            Cons(
                get_relation_context_at_node(path, triple), None
            ),
        ),
        sop,
    )
