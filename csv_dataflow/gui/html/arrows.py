from typing import TypeVar
from csv_dataflow.gui.asserts import assert_isinstance
from csv_dataflow.gui.highlighting import (
    highlight_related_on_hover,
    relation_id_from_path,
)
from csv_dataflow.relation import (
    BasicRelation,
    ParallelChildIndex,
    ParallelRelation,
    Relation,
    RelationPrefix,
)
from csv_dataflow.sop import SumProductPath


S = TypeVar("S")
T = TypeVar("T")


def arrows_div(
    relation: Relation[S, T],
    relation_prefix: RelationPrefix = (),
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
                <div id="{relation_id}" _="{
                    highlight_related_on_hover(
                        relation,
                        relation,
                        relation_prefix,
                        source_prefix,
                        target_prefix
                    )}">
                    {inner_arrows}
                </div>
            """
        case BasicRelation():
            return f"""
                <div id="{relation_id}"
                        _="{
                    highlight_related_on_hover(
                        relation,
                        relation,
                        relation_prefix,
                        source_prefix,
                        target_prefix
                    )}">
                    ⟶
                </div>
            """
        case _:
            raise NotImplementedError


def arrows_html(visible: Relation[S, T]) -> str:
    return f'<div class="arrows">{arrows_div(visible)}</div>'
