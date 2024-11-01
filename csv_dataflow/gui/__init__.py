from dataclasses import replace
import inspect
from itertools import chain, islice
from pathlib import Path
import pickle
from typing import MutableMapping, TypeVar, cast
from flask import Flask, session
import sys

from csv_dataflow.gui.visibility import compute_visible_sop
from examples.ex1.types import A, B

from ..relation import (
    BasicRelation,
    ParallelRelation,
    Relation,
    SeriesRelation,
    parallel_relation_from_csv,
    replace_source_and_target,
)
from ..sop import SumProductNode, map_node_data

typed_session = cast(MutableMapping[str, bytes], session)

app = Flask(__name__)
app.config.update(SECRET_KEY="hello")

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
"""


def sop_html(path: str, sop: SumProductNode[T]) -> str:
    match sop.sop:
        case "+":
            sop_class = "sum"
        case "*":
            sop_class = "product"

    if sop.children:
        return inspect.cleandoc(
            f"""
            <div class="{sop_class}">
                <div>{path}</div>
                <div>
                    {"".join(
                        sop_html(child_path, child)
                        for child_path, child in sop.children.items())}
                </div>
            </div>
        """
        )
    else:
        return f"<div>{path}</div>"


def relation_html(relation: Relation[S, T]) -> str:
    match relation:
        case BasicRelation(source=source, target=target) | ParallelRelation(
            source=source, target=target
        ):
            sop_htmls = (sop_html("Source", source), sop_html("Target", target))
            relation_htmls = ("boo",)
        case SeriesRelation():
            raise NotImplementedError

    return "".join(
        islice(
            chain.from_iterable(zip(chain(("",), relation_htmls), sop_htmls)), 1, None
        )
    )


def html(relation: Relation[S, T]) -> str:
    return inspect.cleandoc(
        f"""
        <!doctype html>
        <html>
            <head><style>{css}</style></head>
            <body>{relation_html(relation)}</body>
        </html>
    """
    )


@app.route("/")
def root() -> str:
    if not typed_session.get("visible"):
        relation = parallel_relation_from_csv(
            A, B, Path("examples/ex1/a_name_to_b_code.csv")
        )

        source_selected = map_node_data(lambda _: False, relation.source)
        target_selected = map_node_data(lambda _: False, relation.target)
        typed_session["selected"] = pickle.dumps(
            replace_source_and_target(relation, source_selected, target_selected)
        )

        source_expanded = map_node_data(lambda _: False, relation.source)
        target_expanded = map_node_data(lambda _: False, relation.target)
        # Expand top level
        source_expanded = replace(source_expanded, data=True)
        target_expanded = replace(target_expanded, data=True)
        typed_session["expanded"] = pickle.dumps(
            replace_source_and_target(relation, source_expanded, target_expanded)
        )

        source_visible = compute_visible_sop(source_selected, source_expanded)
        target_visible = compute_visible_sop(target_selected, target_expanded)
        assert source_visible
        assert target_visible
        typed_session["visible"] = pickle.dumps(
            replace_source_and_target(relation, source_visible, target_visible)
        )

    return html(pickle.loads(typed_session["visible"]))

@app.route("/expanded/<path>", methods=["PUT"])
def expand(path: str) -> str:
    pass


@app.route("/expanded/<path>", methods=["DELETE"])
def collapse(path: str) -> str:
    pass
