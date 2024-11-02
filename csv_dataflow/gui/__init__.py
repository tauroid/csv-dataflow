from dataclasses import replace
import inspect
from itertools import chain, islice
from pathlib import Path
import pickle
import time
from typing import Any, MutableMapping, TypeVar, cast
from flask import Flask, g, Response, session

from csv_dataflow.gui.visibility import compute_visible_relation, compute_visible_sop
from examples.ex1.types import A, B

from ..relation import (
    BasicRelation,
    ParallelRelation,
    Relation,
    RelationPath,
    SeriesRelation,
    filter_relation,
    iter_relation_paths,
    parallel_relation_from_csv,
    replace_source_and_target,
)
from ..sop import SumProductNode, SumProductPath, map_node_data

typed_session = cast(MutableMapping[str, bytes], session)

app = Flask(__name__)
app.config.update(SECRET_KEY="hello")


@app.before_request
def start_timer():
    g.start = time.time()


# Maybe come back to this later but oob swapping not a priority
# right now. Two requests Jackson
@app.after_request
def just_refresh(response: Response) -> Response:
    response.headers["HX-Refresh"] = "true"
    print(f"Response time: {time.time() - g.start:.2E}s")
    return response


S = TypeVar("S")
T = TypeVar("T")

css = """
* {
    box-sizing: border-box;
}
body {
    display: flex;
    margin: 0;
    width: 100vw;
    min-height: 100vh;
    align-items: center;
    justify-content: center;
    gap: 50px;
}
div:not(:has(*)) {
    border: 2px solid black;
    padding: 4px;
    width: 100%;
    background-color: white;
    text-align: center;
    z-index: 1;
    position: relative;
}
.product, .sum {
    display: flex;
    flex-direction: column;
}
.product > div:first-child, .product > div:last-child > div:not(:has(*)) {
    margin-bottom: -2px;
}
.sum > div:last-child
{
    border-left: 2px solid black;
    border-right: 2px solid black;
    background-color: white;
}
.product > div:last-child > .product {
    background-color: black;
}
.sum > div:last-child {
    background-color: #bbb;
}
.sum > div:last-child {
    position: relative;
}
.sum > div:last-child::before {
    content: '';
    position: absolute;
    left: -2px;
    top: calc(50% - 6px);
    width: 4px;
    height: 8px;
    background-color: black;
    border: 2px solid black;
    border-radius: 0 6px 6px 0;
}
.sum > div:last-child > div {
    margin-top: 12px;
    position: relative;
    left: 16px;
    box-shadow: 0px 0px 12px black;
}
.sum > div:last-child > div::before {
    content: '';
    position: absolute;
    left: -6px;
    top: calc(50% - 6px);
    width: 4px;
    height: 8px;
    background-color: white;
    border: 2px solid black;
    border-radius: 6px 0 0 6px;
}
.sum > div:last-child > div:not(:has(*))::before {
    left: -8px;
}
.sum > div:last-child > div:last-child {
    margin-bottom: 12px;
}
.product > div:first-child, .sum > div:first-child {
    background-color: #bbb;
}
.product > div:last-child > .product > div:first-child,
.product > div:last-child > .sum > div:first-child
{
    background-color: #eee;
}
.product > div:last-child > .product > div:first-child:before,
.product > div:last-child > .sum > div:first-child:before
{
    content: '▼';
    position: absolute;
    left: 6px;
}
.product > div:last-child > .product > div:first-child:after,
.product > div:last-child > .sum > div:first-child:after
{
    content: '▼';
    position: absolute;
    right: 6px;
}
.product > div:last-child > .product > div:last-child {
    position: relative;
    left: 8px;
}
.product > div:last-child > .product:last-child > div:last-child {
    border-bottom: 2px solid black;
}
.product > div:last-child > .sum:last-child > div:last-child {
    border-bottom: 2px solid black;
    margin-bottom: -2px;
}
body > .product, body > .sum {
    min-width: 200px;
    width: fit-content;
}
.sum > div:last-child > .sum > div:last-child {
    border-bottom: 2px solid black;
}
body > .sum {
    border-bottom: 2px solid black;
}
.ellipsis {
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: bold;
}
.ellipsis::after {
    content: '⋯';
}
.product > div:last-child > .ellipsis {
    height: 25px;
    box-shadow: inset 0px 3px 3px -3px, inset 0px -3px 3px -3px;
}
.sum .ellipsis::before {
    content: none !important;
}
.sum > div:last-child > .ellipsis {
    border: none;
    background-color: transparent;
    box-shadow: none !important;
    padding: 2px;
    height: 0px;
}
.highlighted {
    background-color: #dfb !important;
}
"""


def sop_html(visible: Relation[S, T, bool], path: RelationPath[S, T]) -> str:
    sop = visible.at(path)
    match sop.sop:
        case "+":
            sop_class = "sum"
        case "*":
            sop_class = "product"

    expand_path = f"/expanded/{path.to_str()}"
    path_id = f"{path.to_str(":")}"
    related_ids = tuple(
        set(f"#{path.to_str(":")}" for path in iter_relation_paths(visible))
    )
    hover = (
        f"on mouseenter toggle .highlighted on [{",".join(related_ids)}] until mouseleave"
        if related_ids
        else ""
    )
    label = path.flat()[-1]

    if sop.children:
        if not path.sop_path:
            # Don't collapse the root
            hx_attrs = ""
        else:
            hx_attrs = f'hx-delete="{expand_path}" hx-swap="outerHTML" hx-target="closest .{sop_class}"'

        child_htmls = (
            sop_html(
                filtered_relation or replace(visible, children=()),
                child_path,
            )
            for child_label in sop.children
            for child_path in (replace(path, sop_path=(*path.sop_path, child_label)),)
            for filtered_relation in (filter_relation(visible, (child_path,)),)
        )

        return inspect.cleandoc(
            f"""
            <div class="{sop_class}">
                <div id="{path_id}" _="{hover}" {hx_attrs}>{label}</div>
                <div>{"".join(child_htmls)}</div>
            </div>
            """
        )
    else:
        return f"""<div id="{path_id}" _="{hover}" hx-put="{expand_path}" hx-swap="outerHTML">{label}</div>"""


def relation_html(visible: Relation[S, T, bool]) -> str:
    match visible:
        case BasicRelation() | ParallelRelation():
            sop_htmls = (
                sop_html(visible, RelationPath("Source", ())),
                sop_html(visible, RelationPath("Target", ())),
            )
            relation_htmls = ("boo",)
        case SeriesRelation():
            raise NotImplementedError

    return "".join(
        islice(
            chain.from_iterable(zip(chain(("",), relation_htmls), sop_htmls)), 1, None
        )
    )


def html(visible: Relation[S, T, bool]) -> str:
    return inspect.cleandoc(
        f"""
        <!doctype html>
        <html>
            <head>
                <style>{css}</style>
                <script src="https://unpkg.com/htmx.org@2.0.3"></script>
                <script src="https://unpkg.com/hyperscript.org@0.9.13"></script>
            </head>
            <body>{relation_html(visible)}</body>
        </html>
    """
    )


@app.route("/")
def root() -> str:
    if not typed_session.get("selected"):
        relation = parallel_relation_from_csv(
            A, B, Path("examples/ex1/a_name_to_b_code.csv")
        )

        source_selected = map_node_data(lambda _: False, relation.source)
        target_selected = map_node_data(lambda _: False, relation.target)
        selected = replace_source_and_target(relation, source_selected, target_selected)
        typed_session["selected"] = pickle.dumps(selected)

        source_expanded = map_node_data(lambda _: False, relation.source)
        target_expanded = map_node_data(lambda _: False, relation.target)
        # Expand top level
        source_expanded = replace(source_expanded, data=True)
        target_expanded = replace(target_expanded, data=True)
        expanded = replace_source_and_target(relation, source_expanded, target_expanded)
        typed_session["expanded"] = pickle.dumps(expanded)
    else:
        selected = pickle.loads(typed_session["selected"])
        expanded = pickle.loads(typed_session["expanded"])

    return html(compute_visible_relation(selected, expanded))


def set_path_expanded(path: RelationPath[A, B], yes: bool) -> Relation[A, B, bool]:
    selected: ParallelRelation[A, B, bool] = pickle.loads(typed_session["selected"])
    expanded: ParallelRelation[A, B, bool] = pickle.loads(
        typed_session["expanded"]
    ).replace_data_at(path, yes)
    typed_session["expanded"] = pickle.dumps(expanded)

    return compute_visible_relation(selected, expanded)


@app.route("/expanded/<path:str_path>", methods=["PUT"])
def expand(str_path: str) -> str:
    path = RelationPath[A, B].from_str(str_path)

    return sop_html(set_path_expanded(path, True), path)


@app.route("/expanded/<path:str_path>", methods=["DELETE"])
def collapse(str_path: str) -> str:
    path = RelationPath[A, B].from_str(str_path)

    return sop_html(set_path_expanded(path, False), path)
