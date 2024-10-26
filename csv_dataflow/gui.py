from functools import partial
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator
from flask import Flask, request
import inspect
import sys

from examples.ex1.types import A, B

from .relation import (
    BasicRelation,
    ParallelRelation,
    Relation,
    SeriesRelation,
    filter_relation,
    iter_source_paths,
    iter_target_paths,
    parallel_relation_from_csv,
)
from .sop import SumProductNode, SumProductPath


app = Flask(__name__)


def path_to_url(path: SumProductPath[Any]) -> str:
    if not path:
        return ""
    else:
        return "/" + "/".join(path)


def path_to_id(
    stage_name: str,
    path: SumProductPath[Any],
    limit: int | None = None,
    pound: bool = False,
) -> str:
    if limit is not None:
        path = path[:limit]

    return ("#" if pound else "") + f"{stage_name}:" + ":".join(path)


def sop_to_html(
    node: SumProductNode[Any],
    stage_name: str,
    current_path: SumProductPath[Any],
    child_path_to_related_ids: Callable[[SumProductPath[Any]], tuple[str, ...]],
) -> str:
    child_paths = {name: (*current_path, name) for name in node.children}
    child_ids = {
        name: f"{stage_name}:" + ":".join(path) for name, path in child_paths.items()
    }
    query_strings = {
        name: "&".join(
            f"{k}={v}"
            for k, v, in {
                **request.args.copy(),
                f"{stage_name}-path": f"{path_to_url(child_paths[name])}/",
            }.items()
        )
        for name in node.children
    }

    return inspect.cleandoc(
        f"""
        <table>
            <thead>
                <tr><th>{node.sop}</th></tr>
            </thead>
            <tbody>
                {"".join(f"""
                    <tr><td id="{child_ids[name]}" _="on mouseenter toggle .highlighted on [{", ".join(("me", *child_path_to_related_ids(child_paths[name])))}] until mouseleave">
                        <a href="?{query_strings[name]}">
                            {name}
                        </a>
                    </td></tr>
                """ for name in node.children)}
            </tbody>
        </table>
        """
    )


def relation_to_html(relation: Relation[Any, Any]) -> str:
    match relation:
        case ParallelRelation(children=children):
            return "".join(relation_to_html(child) for child in children)
        case BasicRelation(source_paths=source_paths, target_paths=target_paths):
            return inspect.cleandoc(
                f"""
                <p>{source_paths} -> {target_paths}
                """
            )
        case SeriesRelation():
            raise NotImplementedError


@app.route("/")
def dump():
    css = inspect.cleandoc(
        """
        table { border-collapse: collapse; }
        table td, table th {
            border: 1px solid grey;
        }
        .highlighted {
            background-color: #ddffbb;
        }
        body {
            margin: 0;
            height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
        }
        main {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 20px;
        }
        """
    )

    rel = parallel_relation_from_csv(A, B, Path("examples/ex1/a_name_to_b_code.csv"))

    print(rel, file=sys.stderr)

    a_path = tuple(request.args.get("a-path", default="").split("/")[1:-1])
    b_path = tuple(request.args.get("b-path", default="").split("/")[1:-1])

    a_sop = rel.source.at(a_path)
    b_sop = rel.target.at(b_path)

    def filter_visible(
        paths: Iterable[SumProductPath[Any]],
        sop: SumProductNode[Any],
        path_of_sop: SumProductPath[Any],
    ) -> Iterator[SumProductPath[Any]]:
        return filter(
            lambda p: len(p) > len(path_of_sop)
            and p[: len(path_of_sop)] == path_of_sop
            and p[len(path_of_sop)] in sop.children,
            paths,
        )

    def a_related(source_path: SumProductPath[Any]) -> tuple[str, ...]:
        f_rel = filter_relation(rel, (source_path,), None)
        visible_target_paths = filter_visible(iter_target_paths(f_rel), b_sop, b_path)
        return (
            "#beans",
            *set(
                map(
                    partial(path_to_id, "b", limit=len(b_path) + 1, pound=True),
                    visible_target_paths,
                )
            ),
        )

    def b_related(target_path: SumProductPath[Any]) -> tuple[str, ...]:
        f_rel = filter_relation(rel, None, (target_path,))
        visible_source_paths = filter_visible(iter_source_paths(f_rel), a_sop, a_path)
        return (
            "#beans",
            *set(
                map(
                    partial(path_to_id, "a", limit=len(a_path) + 1, pound=True),
                    visible_source_paths,
                )
            ),
        )

    return inspect.cleandoc(
        f"""
        <!doctype html />
        <html>
            <head>
                <script src="https://unpkg.com/htmx.org@2.0.3" integrity="sha384-0895/pl2MU10Hqc6jd4RvrthNlDiE9U1tWmX7WRESftEDRosgxNsQG/Ze9YMRzHq" crossorigin="anonymous"></script>
                <script src="https://unpkg.com/hyperscript.org@0.9.13"></script>
                <style>{css}</style>
            </head>
            <body>
            <main>
                <div>{sop_to_html(a_sop, "a", a_path, a_related)}</div>
                <div>{relation_to_html(rel)}</div>
                <div>{sop_to_html(b_sop, "b", b_path, b_related)}</div>
                <button hx-get="/beans" hx-target="#beans">Beans</button>
                <p id="beans"></p>
            </main>
            </body>
        </html>
        """
    )


@app.route("/beans")
def beans():
    return "beans"
