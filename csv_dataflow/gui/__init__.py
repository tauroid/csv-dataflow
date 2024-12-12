from dataclasses import replace
from functools import partial
import inspect
from itertools import accumulate, chain, islice
from pathlib import Path
import pickle
import time
from typing import Any, Literal, MutableMapping, TypeVar, cast
from flask import Flask, g, Response, session

from pprint import pprint

from examples.ex1.types import A, B

from ..csv import parallel_relation_from_csv
from ..relation import (
    BasicRelation,
    ParallelRelation,
    Relation,
    RelationPath,
    SeriesRelation,
    clip_relation,
    filter_relation,
    iter_basic_relations,
    iter_relation_paths,
)
from ..sop import DeBruijn, SumProductNode, SumProductPath, map_node_data, sop_from_type

from .visibility import compute_visible_sop

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
.sum div:not(:has(*)), .product div:not(:has(*)) {
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
.product > div.highlighted:first-child, .sum > div.highlighted:first-child {
    background-color: #a0b099 !important;
}
.product > div:last-child > .product > div:first-child,
.product > div:last-child > .sum > div:first-child
{
    background-color: #eee;
}
.product > div:last-child > .product > div.highlighted:first-child,
.product > div:last-child > .sum > div.highlighted:first-child
{
    background-color: #dec !important;
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
.arrows {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    font-size: 30pt;
    gap: 20px;
}
.arrows > div {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding-bottom: 6px;
    height: 35px;
    width: 75px;
    border-radius: 5px;
    overflow: hidden;
}
.arrows .highlighted {
    background-color: #dfb !important;
    border: 2px solid black;
}
"""


def basic_relation_id(relation: BasicRelation[S, T]) -> str:
    return f"rel-{hash(relation)}"


def highlight_related_on_hover(relation: Relation[S, T]) -> str:
    relation_paths = filter(
        lambda p: len(p) > 1,
        chain.from_iterable(
            accumulate(map(lambda x: (x,), path.flat()))
            for path in iter_relation_paths(relation)
        ),
    )
    related_ids = (f"#{":".join(map(str, path))}" for path in set(relation_paths))
    basic_relation_ids = set(
        map(lambda r: f"#{basic_relation_id(r)}", iter_basic_relations(relation))
    )
    return (
        f"on mouseenter toggle .highlighted on [{",".join(chain(related_ids, basic_relation_ids))}] until mouseleave"
        if related_ids
        else ""
    )


def sop_html(
    sop: SumProductNode[Any, bool] | DeBruijn,
    relation: Relation[S, T],
    path: RelationPath[S, T],
) -> str:
    expand_path = f"/expanded/{path.to_str()}"
    path_id = f"{path.to_str(":")}"

    hover = highlight_related_on_hover(relation) if path.sop_path else ""

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
                filtered_relation = filter_relation(relation, (child_path,))
                yield sop_html(
                    child,
                    filtered_relation or replace(relation, children=()),
                    child_path,
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


def arrow_div(basic_relation: BasicRelation[S, T]) -> str:
    return f"""
       <div id="{basic_relation_id(basic_relation)}"
             _="{highlight_related_on_hover(basic_relation)}">
         ⟶
       </div>
    """


# FIXME time for this to be recursive
# iter_basic_relations doesn't play well with Between
# (unit BasicRelations look the same but have different targets)
def arrows_html(visible: Relation[S, T]) -> str:
    return f"""
        <div class="arrows">
            {"".join(arrow_div(basic_relation) for basic_relation in dict(
                (r,None) for r in iter_basic_relations(visible)
             ))}
        </div>
    """


def relation_html(
    source: SumProductNode[S, bool],
    target: SumProductNode[T, bool],
    relation: Relation[S, T],
) -> str:
    match relation:
        case BasicRelation() | ParallelRelation():
            sop_htmls = (
                sop_html(source, relation, RelationPath("Source", ())),
                sop_html(target, relation, RelationPath("Target", ())),
            )
            arrows = arrows_html(relation)
        case SeriesRelation():
            raise NotImplementedError

    return "".join(
        islice(chain.from_iterable(zip(chain(("",), (arrows,)), sop_htmls)), 1, None)
    )


def html(
    source: SumProductNode[S, bool],
    target: SumProductNode[T, bool],
    relation: Relation[S, T],
) -> str:
    """Args are only the parts to actually display on the page"""
    return inspect.cleandoc(
        f"""
        <!doctype html>
        <html>
            <head>
                <style>{css}</style>
                <script src="https://unpkg.com/htmx.org@2.0.3"></script>
                <script src="https://unpkg.com/hyperscript.org@0.9.13"></script>
            </head>
            <body>{relation_html(source, target, relation)}</body>
        </html>
    """
    )


from examples.ex3.precompiled_list import sop, relation


@app.route("/")
def root() -> str:
    if not typed_session.get("relation"):
        # relation_1 = parallel_relation_from_csv(
        #     A, B, Path("examples/ex1/a_name_to_b_code.csv")
        # )
        # source, target, relation = parallel_relation_from_csv(
        #     A, B, Path("examples/ex1/a_name_to_b_option.csv")
        # )
        global sop
        source = sop
        target = sop
        global relation
        relation = relation
        # relation = ParallelRelation(
        #     relation_1.source,
        #     relation_1.target,
        #     (relation_1, relation_2)
        # )
        typed_session["relation"] = pickle.dumps(relation)

        source_selected = map_node_data(lambda _: False, source)
        target_selected = map_node_data(lambda _: False, target)
        typed_session["source_selected"] = pickle.dumps(source_selected)
        typed_session["target_selected"] = pickle.dumps(target_selected)

        source_expanded = map_node_data(lambda _: False, source)
        target_expanded = map_node_data(lambda _: False, target)
        # Expand top level
        source_expanded = replace(source_expanded, data=True)
        target_expanded = replace(target_expanded, data=True)
        typed_session["source_expanded"] = pickle.dumps(source_expanded)
        typed_session["target_expanded"] = pickle.dumps(target_expanded)
    else:
        relation = pickle.loads(typed_session["relation"])
        source_selected = pickle.loads(typed_session["source_selected"])
        target_selected = pickle.loads(typed_session["target_selected"])
        source_expanded = pickle.loads(typed_session["source_expanded"])
        target_expanded = pickle.loads(typed_session["target_expanded"])

    return html(
        *recalculate_session_visible_relation(
            relation, source_selected, target_selected, source_expanded, target_expanded
        )
    )


def recalculate_session_visible_relation(
    relation: Relation[S, T],
    source_selected: SumProductNode[S, bool],
    target_selected: SumProductNode[T, bool],
    source_expanded: SumProductNode[S, bool],
    target_expanded: SumProductNode[T, bool],
) -> tuple[SumProductNode[S, bool], SumProductNode[T, bool], Relation[S, T]]:
    source_visible = compute_visible_sop(source_selected, source_expanded)
    target_visible = compute_visible_sop(target_selected, target_expanded)
    assert source_visible
    assert target_visible
    typed_session["source_visible"] = pickle.dumps(source_visible)
    typed_session["target_visible"] = pickle.dumps(target_visible)
    visible_relation = clip_relation(relation, source_visible, target_visible)
    print(visible_relation)
    typed_session["visible_relation"] = pickle.dumps(visible_relation)
    return source_visible, target_visible, visible_relation


def set_session_point_path_expanded(
    point: Literal["Source", "Target"], path: SumProductPath[Any], yes: bool = True
) -> tuple[SumProductNode[Any, bool], Relation[Any, Any]]:
    """Returns new visible sop and relation"""
    expanded_key = f"{point.lower()}_expanded"
    expanded: SumProductNode[Any, bool] = pickle.loads(typed_session[expanded_key])
    expanded = expanded.replace_data_at(path, yes)
    if yes:
        # Expand recursion
        for child in expanded.at(path).children:
            child_path = (*path, child)
            expanded = expanded.replace_at(
                child_path, map_node_data(lambda _: False, expanded.at(child_path))
            )
    typed_session[expanded_key] = pickle.dumps(expanded)
    match point:
        case "Source":
            recalculate = partial(
                recalculate_session_visible_relation,
                source_expanded=expanded,
                target_expanded=pickle.loads(typed_session["target_expanded"]),
            )
        case "Target":
            recalculate = partial(
                recalculate_session_visible_relation,
                source_expanded=pickle.loads(typed_session["source_expanded"]),
                target_expanded=expanded,
            )

    s, t, r = recalculate(
        pickle.loads(typed_session["relation"]),
        pickle.loads(typed_session["source_selected"]),
        pickle.loads(typed_session["target_selected"]),
    )

    match point:
        case "Source":
            return s, r
        case "Target":
            return t, r


def set_session_path_expanded(
    path: RelationPath[Any, Any], yes: bool = True
) -> tuple[SumProductNode[Any, bool], Relation[Any, Any]]:
    return set_session_point_path_expanded(path.point, path.sop_path, yes)


@app.route("/expanded/<path:str_path>", methods=["PUT"])
def expand(str_path: str) -> str:
    path = RelationPath[A, B].from_str(str_path)

    return sop_html(*set_session_path_expanded(path), path)


@app.route("/expanded/<path:str_path>", methods=["DELETE"])
def collapse(str_path: str) -> str:
    path = RelationPath[A, B].from_str(str_path)

    return sop_html(*set_session_path_expanded(path, False), path)
