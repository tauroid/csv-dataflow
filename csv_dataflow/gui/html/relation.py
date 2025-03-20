import inspect
from itertools import chain, islice
from pathlib import Path
from typing import TypeVar
from csv_dataflow.gui.html.arrows import arrows_html
from csv_dataflow.gui.html.sop import sop_html
from csv_dataflow.gui.state.triple import VisibleTriple
from csv_dataflow.relation import (
    BasicRelation,
    ParallelRelation,
    Relation,
    RelationPath,
    SeriesRelation,
)
from csv_dataflow.sop import SumProductNode

S = TypeVar("S")
T = TypeVar("T")


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
                    page_name,
                    source,
                    RelationPath("Source", ()),
                    relation,
                    relation,
                ),
                sop_html(
                    page_name,
                    target,
                    RelationPath("Target", ()),
                    relation,
                    relation,
                ),
            )
            arrows = arrows_html(relation)
        case SeriesRelation():
            raise NotImplementedError

    return "".join(
        islice(
            chain.from_iterable(
                zip(chain(("",), (arrows,)), sop_htmls)
            ),
            1,
            None,
        )
    )


css = (Path(__file__).parent / "style.css").read_text()


def relation_page_html(
    page_name: str, visible: VisibleTriple[S, T]
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
            <body>{relation_html(page_name, visible.source, visible.target, visible.relation)}</body>
        </html>
    """
    )
