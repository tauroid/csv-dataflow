from pathlib import Path
import time
from typing import Any, MutableMapping, TypeVar, cast
from flask import Flask, g, Response, session
from flask_session import Session
from cachelib.file import FileSystemCache

from pprint import pprint

from csv_dataflow.gui.html.relation import (
    relation_page_html,
)
from csv_dataflow.gui.html.sop import sop_div
from csv_dataflow.gui.path_expansion import (
    set_session_path_expanded,
)
from csv_dataflow.gui.state.pickler import attach_pickle_store
from csv_dataflow.gui.state.triple import (
    TripleState,
)
from examples.ex1.types import A, B

from ..csv import parallel_relation_from_csv
from ..relation import (
    RelationPath,
    Triple,
)

typed_session = cast(MutableMapping[str, bytes], session)

app = Flask(__name__)

SESSION_TYPE = "cachelib"
SESSION_SERIALIZATION_FORMAT = "json"
SESSION_CACHELIB = FileSystemCache(
    threshold=500, cache_dir=".sessions"
)
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


def relation_page(name: str, triple: Triple[S, T]) -> str:
    state = TripleState[S, T].from_triple(triple)
    attach_pickle_store(state, typed_session, name)
    return relation_page_html(name, state.visible)


@app.route("/")
def root() -> str:
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
        parallel_relation_from_csv(
            A, B, Path("examples/ex1/a_name_to_b_code.csv")
        ),
    )


@app.route("/ex1-name-to-option")
def example_1_name_to_option() -> str:
    return relation_page(
        "ex1-name-to-option",
        parallel_relation_from_csv(
            A, B, Path("examples/ex1/a_name_to_b_option.csv")
        ),
    )


@app.route("/ex3")
def example_3() -> str:
    from examples.ex3.precompiled_list import sop, relation

    return relation_page("ex3", Triple(sop, sop, relation))


@app.route("/ex4")
def example_4() -> str:
    from examples.ex4.mapflip import sop, relation

    return relation_page("ex4", Triple(sop, sop, relation))


@app.route("/ex5")
def example_5() -> str:
    from examples.ex5.flip import flip

    return relation_page("ex5", flip.as_triple)


@app.route(
    "/<page_name>/expanded/<path:str_path>", methods=["PUT"]
)
def expand(page_name: str, str_path: str) -> str:
    path = RelationPath[Any, Any].from_str(str_path)
    sop, relation = set_session_path_expanded(
        typed_session, page_name, path, True
    )
    return sop_div(page_name, sop, path, relation)


@app.route(
    "/<page_name>/expanded/<path:str_path>", methods=["DELETE"]
)
def collapse(page_name: str, str_path: str) -> str:
    path = RelationPath[Any, Any].from_str(str_path)
    sop, relation = set_session_path_expanded(
        typed_session, page_name, path, False
    )

    return sop_div(page_name, sop, path, relation)
