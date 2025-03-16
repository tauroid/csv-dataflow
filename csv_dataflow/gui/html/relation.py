from itertools import chain, islice
from typing import TypeVar
from csv_dataflow.gui.html.arrows import arrows_html
from csv_dataflow.gui.html.sop import sop_html
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
