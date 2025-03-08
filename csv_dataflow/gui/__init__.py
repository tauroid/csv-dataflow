from dataclasses import replace
from functools import partial
import inspect
from itertools import accumulate, chain, islice, repeat
from pathlib import Path
import pickle
import time
from typing import Any, Literal, MutableMapping, TypeIs, TypeVar, cast
from flask import Flask, g, Response, session
from flask_session import Session
from cachelib.file import FileSystemCache

from pprint import pprint

from csv_dataflow.relation.clipping import clip_relation
from csv_dataflow.relation.filtering import filter_relation
from csv_dataflow.relation.iterators import iter_relation_paths
from examples.ex1.types import A, B

from ..csv import parallel_relation_from_csv
from ..relation import (
    BasicRelation,
    ParallelChildIndex,
    ParallelRelation,
    Relation,
    RelationPath,
    RelationPathElement,
    SeriesRelation,
)
from ..sop import (
    DeBruijn,
    SumProductNode,
    SumProductPath,
    map_node_data,
)

from .visibility import compute_visible_sop

typed_session = cast(MutableMapping[str, bytes], session)

app = Flask(__name__)

SESSION_TYPE = "cachelib"
SESSION_SERIALIZATION_FORMAT = "json"
SESSION_CACHELIB = FileSystemCache(threshold=500, cache_dir=".sessions")
app.config.from_object(__name__)
Session(app)


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
.arrows div {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 6px;
}
.arrows div:has(div:not(:only-child)) {
    border: 2px solid black;
    padding: 6px;
    border-radius: 10px;
}
.arrows div:not(:has(div)) {
    height: 35px;
    width: 75px;
    overflow: hidden;
    border-radius: 5px;
    padding-bottom: 2px;
}
.arrows div.highlighted {
    background-color: #dfb !important;
    border: 2px solid black;
}
"""


def basic_relation_id(relation: BasicRelation[S, T]) -> str:
    return f"rel-{hash(relation)}"


def relation_id_from_path(path: tuple[RelationPathElement, ...]) -> str:
    return f"rel_:{":".join(map(str, path))}"


# FIXME using relation and filtered_relation, compute the
# smallest set of relation ids that unambiguously show which
# relations are in filtered_relation
#
# so if all children recursively of some relation in `relation`
# are also in `filtered_relation`, just give the id of that
# top relation instead of all the children
#
# then highlighting that top relation will be sufficient


def assert_true(value: bool) -> bool:
    assert value
    return True


def assert_isinstance(value: Any, typ: type[T], yes: bool = True) -> TypeIs[T]:
    """
    Make `yes` false if you want to assert it's not an instance
    then return False
    """
    assert isinstance(value, typ) == yes
    return yes


def relation_ids_to_highlight(
    filtered_relation: Relation[S, T],
    full_relation: Relation[S, T] | None = None,
    prefix: tuple[RelationPathElement, ...] = (),
) -> tuple[str, ...] | None:
    """
    None means no discrepancy yet between filtered_relation and full_relation

    So if the top returns None, highlight the top relation's id
    """
    match filtered_relation:
        case BasicRelation(source, target):
            if source is None and target is None:
                return ()

            if full_relation is None:
                return (relation_id_from_path(prefix),)

            # Both are populated and we assume from the structural
            # recursion and how filtering works on BasicRelations
            # that they are the same
            assert filtered_relation == full_relation
            return None
        case ParallelRelation(children):
            assert isinstance(full_relation, ParallelRelation) or full_relation is None
            child_relation_ids = tuple(
                relation_ids_to_highlight(
                    filtered_child, full_child, (*prefix, ParallelChildIndex(i))
                )
                for i, (
                    (filtered_child, filtered_between),
                    (full_child, full_between),
                ) in enumerate(
                    zip(
                        children,
                        (
                            full_relation.children
                            if full_relation is not None
                            else repeat((None, None))
                        ),
                    )
                )
                if assert_true(full_between is None or filtered_between == full_between)
                and not assert_isinstance(filtered_child, int, False)
                and not assert_isinstance(full_child, int, False)
            )
            if all(ids is None for ids in child_relation_ids):
                return None
            else:
                return tuple(
                    chain.from_iterable(
                        (
                            ids
                            if ids is not None
                            else (
                                relation_id_from_path((*prefix, ParallelChildIndex(i))),
                            )
                        )
                        for i, ids in enumerate(child_relation_ids)
                    )
                )
        case SeriesRelation():
            raise NotImplementedError


def highlight_related_on_hover(
    filtered_relation: Relation[S, T],
    full_relation: Relation[S, T] | None = None,
    relation_prefix: tuple[RelationPathElement, ...] = (),
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
) -> str:
    relation_paths = filter(
        lambda p: len(p) > 1,
        chain.from_iterable(
            accumulate(map(lambda x: (x,), path.flat()))
            for path in iter_relation_paths(
                filtered_relation, source_prefix, target_prefix
            )
        ),
    )
    related_ids = (f"#{":".join(map(str, path))}" for path in set(relation_paths))
    relation_ids = relation_ids_to_highlight(
        filtered_relation, full_relation, relation_prefix
    )
    if relation_ids is not None:
        relation_ids = map(lambda relation_id: f"#{relation_id}", relation_ids)
    else:
        relation_ids = (f"#{relation_id_from_path(relation_prefix)}",)
    return (
        f"on mouseover halt the event's bubbling toggle .highlighted on [{",".join(chain(related_ids, relation_ids))}] until mouseout"
        if related_ids
        else ""
    )


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


def arrows_div(
    relation: Relation[S, T],
    relation_prefix: tuple[RelationPathElement, ...] = (),
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
) -> str:
    relation_id = relation_id_from_path(relation_prefix)
    match relation:
        case ParallelRelation(children):
            inner_arrows = "".join(
                arrows_div(
                    child,
                    (*relation_prefix, ParallelChildIndex(i)),
                    (*source_prefix, *between.source),
                    (*target_prefix, *between.target),
                )
                for i, (child, between) in enumerate(children)
                # Shouldn't happen as should be expanded
                if not assert_isinstance(child, int, False)
            )
            return f"""
                <div id="{relation_id}" _="{highlight_related_on_hover(relation, relation, relation_prefix, source_prefix, target_prefix)}">
                {inner_arrows}
                </div>
            """
        case BasicRelation():
            return f"""
                <div id="{relation_id}"
                        _="{highlight_related_on_hover(relation, relation, relation_prefix, source_prefix, target_prefix)}">
                    ⟶
                </div>
            """
        case _:
            raise NotImplementedError


def arrows_html(visible: Relation[S, T]) -> str:
    return f'<div class="arrows">{arrows_div(visible)}</div>'


def relation_html(
    page_name: str,
    source: SumProductNode[S, bool],
    target: SumProductNode[T, bool],
    relation: Relation[S, T],
) -> str:
    match relation:
        case BasicRelation() | ParallelRelation():
            sop_htmls = (
                sop_html(
                    page_name, source, RelationPath("Source", ()), relation, relation
                ),
                sop_html(
                    page_name, target, RelationPath("Target", ()), relation, relation
                ),
            )
            arrows = arrows_html(relation)
        case SeriesRelation():
            raise NotImplementedError

    return "".join(
        islice(chain.from_iterable(zip(chain(("",), (arrows,)), sop_htmls)), 1, None)
    )


def html(
    page_name: str,
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
            <body>{relation_html(page_name, source, target, relation)}</body>
        </html>
    """
    )


def relation_page(
    name: str,
    source: SumProductNode[S],
    target: SumProductNode[T],
    relation: Relation[S, T],
) -> str:
    if not typed_session.get(f"{name}_relation"):
        typed_session[f"{name}_relation"] = pickle.dumps(relation)

        source_selected = map_node_data(lambda _: False, source)
        target_selected = map_node_data(lambda _: False, target)
        typed_session[f"{name}_source_selected"] = pickle.dumps(source_selected)
        typed_session[f"{name}_target_selected"] = pickle.dumps(target_selected)

        source_expanded = map_node_data(lambda _: False, source)
        target_expanded = map_node_data(lambda _: False, target)
        # Expand top level
        source_expanded = replace(source_expanded, data=True)
        target_expanded = replace(target_expanded, data=True)
        typed_session[f"{name}_source_expanded"] = pickle.dumps(source_expanded)
        typed_session[f"{name}_target_expanded"] = pickle.dumps(target_expanded)
    else:
        relation = pickle.loads(typed_session[f"{name}_relation"])
        source_selected = pickle.loads(typed_session[f"{name}_source_selected"])
        target_selected = pickle.loads(typed_session[f"{name}_target_selected"])
        source_expanded = pickle.loads(typed_session[f"{name}_source_expanded"])
        target_expanded = pickle.loads(typed_session[f"{name}_target_expanded"])

    return html(
        name,
        *recalculate_session_visible_relation(
            name,
            relation,
            source_selected,
            target_selected,
            source_expanded,
            target_expanded,
        ),
    )


@app.route("/")
def root() -> str:
    # relation_1 = parallel_relation_from_csv(
    #     A, B, Path("examples/ex1/a_name_to_b_code.csv")
    # )
    # source, target, relation = parallel_relation_from_csv(
    #     A, B, Path("examples/ex1/a_name_to_b_option.csv")
    # )
    # relation = ParallelRelation(
    #     relation_1.source,
    #     relation_1.target,
    #     (relation_1, relation_2)
    # )

    return """
    <!doctype html>
    <html>
        <head>
            <style>
                body {
                    margin: 0;
                    height: 100vh;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }
                page-links {
                    display: flex;
                    flex-direction: column;
                    gap: 20px;
                }
            </style>
        </head>
        <body>
            <page-links>
                <page-link><a href="/ex1-name-to-code">Example 1: Name to Code</a></page-link>
                <page-link><a href="/ex1-name-to-option">Example 1: Name to Option</a></page-link>
                <page-link><a href="/ex3">Example 3: List head</a></page-link>
                <page-link><a href="/ex4">Example 4: List map</a></page-link>
                <page-link><a href="/ex5">Example 5: flip.py</a></page-link>
            </page-links>
        </body>
    </html>
    """


@app.route("/ex1-name-to-code")
def example_1_name_to_code() -> str:
    return relation_page(
        "ex1-name-to-code",
        *parallel_relation_from_csv(A, B, Path("examples/ex1/a_name_to_b_code.csv")),
    )


@app.route("/ex1-name-to-option")
def example_1_name_to_option() -> str:
    return relation_page(
        "ex1-name-to-option",
        *parallel_relation_from_csv(A, B, Path("examples/ex1/a_name_to_b_option.csv")),
    )


@app.route("/ex3")
def example_3() -> str:
    from examples.ex3.precompiled_list import sop, relation

    return relation_page("ex3", sop, sop, relation)


@app.route("/ex4")
def example_4() -> str:
    from examples.ex4.mapflip import sop, relation

    return relation_page("ex4", sop, sop, relation)


@app.route("/ex5")
def example_5() -> str:
    from examples.ex5.flip import flip

    return relation_page("ex5", *flip.as_sops_and_relation)


def recalculate_session_visible_relation(
    name: str,
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
    typed_session[f"{name}_source_visible"] = pickle.dumps(source_visible)
    typed_session[f"{name}_target_visible"] = pickle.dumps(target_visible)
    visible_relation = clip_relation(relation, source_visible, target_visible)
    typed_session[f"{name}_visible_relation"] = pickle.dumps(visible_relation)
    return source_visible, target_visible, visible_relation


def set_session_point_path_expanded(
    name: str,
    point: Literal["Source", "Target"],
    path: SumProductPath[Any],
    yes: bool = True,
) -> tuple[SumProductNode[Any, bool], Relation[Any, Any]]:
    """Returns new visible sop and relation"""
    expanded_key = f"{name}_{point.lower()}_expanded"
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
                name,
                source_expanded=expanded,
                target_expanded=pickle.loads(typed_session[f"{name}_target_expanded"]),
            )
        case "Target":
            recalculate = partial(
                recalculate_session_visible_relation,
                name,
                source_expanded=pickle.loads(typed_session[f"{name}_source_expanded"]),
                target_expanded=expanded,
            )

    s, t, r = recalculate(
        pickle.loads(typed_session[f"{name}_relation"]),
        pickle.loads(typed_session[f"{name}_source_selected"]),
        pickle.loads(typed_session[f"{name}_target_selected"]),
    )

    match point:
        case "Source":
            return s, r
        case "Target":
            return t, r


def set_session_path_expanded(
    name: str, path: RelationPath[Any, Any], yes: bool = True
) -> tuple[SumProductNode[Any, bool], Relation[Any, Any]]:
    return set_session_point_path_expanded(name, path.point, path.sop_path, yes)


@app.route("/<page_name>/expanded/<path:str_path>", methods=["PUT"])
def expand(page_name: str, str_path: str) -> str:
    path = RelationPath[A, B].from_str(str_path)
    sop, relation = set_session_path_expanded(page_name, path)

    print(page_name)

    return sop_html(page_name, sop, path, relation)


@app.route("/<page_name>/expanded/<path:str_path>", methods=["DELETE"])
def collapse(page_name: str, str_path: str) -> str:
    path = RelationPath[A, B].from_str(str_path)
    sop, relation = set_session_path_expanded(page_name, path, False)

    return sop_html(page_name, sop, path, relation)
