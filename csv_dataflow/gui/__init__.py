from dataclasses import replace
from functools import partial
import inspect
from itertools import accumulate, chain, islice, repeat
from pathlib import Path
import pickle
import time
from typing import Any, Literal, MutableMapping, TypeVar, cast
from flask import Flask, g, Response, session
from flask_session import Session
from cachelib.file import FileSystemCache

from pprint import pprint

from csv_dataflow.gui.highlighting import highlight_related_on_hover
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
    Triple,
)
from ..sop import (
    DeBruijn,
    SumProductNode,
    SumProductPath,
    map_node_data,
)

from .visibility import compute_visible_sop
from .asserts import assert_isinstance

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

css = (Path(__file__).parent / "style.css").read_text()


# v I don't actually know what this means, maybe future me v
# v             can make some sense of it                  v
# PLAN using relation and filtered_relation, compute the
# smallest set of relation ids that unambiguously show which
# relations are in filtered_relation
#
# so if all children recursively of some relation in `relation`
# are also in `filtered_relation`, just give the id of that
# top relation instead of all the children
#
# then highlighting that top relation will be sufficient





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
    triple: Triple[S,T]
) -> str:

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
