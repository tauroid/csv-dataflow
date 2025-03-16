from dataclasses import replace
import inspect
from typing import Any, TypeVar
from csv_dataflow.gui.highlighting import highlight_related_on_hover
from csv_dataflow.relation import Relation, RelationPath
from csv_dataflow.relation.filtering import filter_relation
from csv_dataflow.sop import DeBruijn, SumProductNode

S = TypeVar("S")
T = TypeVar("T")


def sop_html(
    page_name: str,
    sop: SumProductNode[Any, bool] | DeBruijn,
    path: RelationPath[S, T],
    filtered_relation: Relation[S, T],
    full_relation: Relation[S, T] | None = None,
) -> str:
    expand_path = f"{page_name}/expanded/{path.to_str()}"
    path_id = f"{path.to_str(":")}"

    hover = (
        highlight_related_on_hover(filtered_relation, full_relation)
        if path.sop_path
        else ""
    )

    label = path.flat()[-1]

    if isinstance(sop, SumProductNode) and sop.children:
        match sop.sop:
            case "+":
                sop_class = "sum"
            case "*":
                sop_class = "product"

        if not path.sop_path:
            # Don't collapse the root
            hx_attrs = ""
        else:
            hx_attrs = f'hx-delete="{expand_path}" hx-swap="outerHTML" hx-target="closest .{sop_class}"'

        def iter_child_htmls():
            for child_label, child in sop.children.items():
                child_path = replace(path, sop_path=(*path.sop_path, child_label))
                filtered_relation_for_child = filter_relation(
                    filtered_relation, (child_path,)
                )
                yield sop_html(
                    page_name,
                    child,
                    child_path,
                    filtered_relation_for_child
                    or replace(filtered_relation, children=()),
                    full_relation,
                )

        return inspect.cleandoc(
            f"""
            <div class="{sop_class}">
                <div id="{path_id}" _="{hover}" {hx_attrs}>{label}</div>
                <div>{"".join(iter_child_htmls())}</div>
            </div>
            """
        )
    else:
        return f"""<div id="{path_id}" _="{hover}" hx-put="{expand_path}" hx-swap="outerHTML">{label}</div>"""
