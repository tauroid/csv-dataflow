import inspect
from itertools import chain, islice
from pathlib import Path
from typing import TypeVar
from csv_dataflow.gui.html.arrows import arrows_html
from csv_dataflow.gui.html.sop import sop_div
from csv_dataflow.gui.state.triple import VisibleTriple
from csv_dataflow.relation import (
    BasicRelation,
    ParallelRelation,
    Relation,
    RelationPath,
    SeriesRelation,
)
from csv_dataflow.relation.triple import relation_to_triple
from csv_dataflow.sop import SumProductNode

from csv_dataflow.gui.html.sop2 import sop_html
from csv_dataflow.gui.html.arrows2 import (
    arrows_html as arrows_html2,
)

S = TypeVar("S")
T = TypeVar("T")


def relation_html(
    page_name: str,
    source: SumProductNode[S],
    target: SumProductNode[T],
    relation: Relation[S, T],
) -> str:
    match relation:
        case BasicRelation() | ParallelRelation():
            match relation:
                case BasicRelation():
                    triple = relation_to_triple(
                        relation, source, target
                    )
                case ParallelRelation():
                    triple = relation_to_triple(
                        relation, source, target
                    )
            sop_htmls = (
                sop_html(page_name, triple, "Source", source),
                sop_html(page_name, triple, "Target", target),
                # sop_div(
                #     page_name,
                #     source,
                #     RelationPath("Source", ()),
                #     relation,
                #     relation,
                # ),
                # sop_div(
                #     page_name,
                #     target,
                #     RelationPath("Target", ()),
                #     relation,
                #     relation,
                # ),
            )
            # arrows = arrows_html(relation)
            arrows = arrows_html2(triple)
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
