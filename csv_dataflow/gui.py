from pathlib import Path
from typing import Any
from flask import Flask, request
import inspect

from examples.ex1.types import A, B

from .relation import (
    BasicRelation,
    ParallelRelation,
    Relation,
    SeriesRelation,
    parallel_relation_from_csv,
)
from .sop import SumProductNode, SumProductPath, sop_from_type


app = Flask(__name__)

def path_to_url(path: SumProductPath[Any]) -> str:
    if not path:
        return ""
    else:
        return "/"+"/".join(path)

def sop_to_html(
    node: SumProductNode[Any],
    path_param: str,
    current_path: SumProductPath[Any],
    other_path_params: dict[str, SumProductPath[Any]],
) -> str:
    other_params_str = "&".join(f"{k}={path_to_url(v)}/" for k, v in other_path_params.items())
    return inspect.cleandoc(
        f"""
        <table>
            <thead>
                <tr><th>{node.sop}</th></tr>
            </thead>
            <tbody>
                {"".join(f"""
                    <tr><td>
                        <a href=?{path_param}={path_to_url(current_path)}/{name}/&{other_params_str}>
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

    a_path = tuple(request.args.get("a-path", default="").split("/")[1:-1])
    b_path = tuple(request.args.get("b-path", default="").split("/")[1:-1])

    return inspect.cleandoc(
        f"""
        <!doctype html />
        <html>
            <head>
                <script src="https://unpkg.com/htmx.org@2.0.3" integrity="sha384-0895/pl2MU10Hqc6jd4RvrthNlDiE9U1tWmX7WRESftEDRosgxNsQG/Ze9YMRzHq" crossorigin="anonymous"></script>
                <style>{css}</style>
            </head>
            <body>
            <main>
                <div>{sop_to_html(rel.source.at(a_path),"a-path",a_path,{"b-path":b_path})}</div>
                <div>{relation_to_html(rel)}</div>
                <div>{sop_to_html(rel.target.at(b_path),"b-path",b_path,{"a-path":a_path})}</div>
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
