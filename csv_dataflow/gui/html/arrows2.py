import inspect
from csv_dataflow.gui.asserts import assert_isinstance
from csv_dataflow.gui.highlighting.hyperscript import hyperscript
from csv_dataflow.gui.highlighting.triple import highlight_triple
from csv_dataflow.relation import (
    DeBruijn,
    ParallelChildIndex,
    ParallelRelation,
    Relation,
    RelationPath,
)
from csv_dataflow.relation.triple import (
    BasicTriple,
    CopyTriple,
    ParallelTriple,
    Triple,
)


def arrows_div[S, T](triple: Triple[S, T, bool]) -> str:
    relation_id = RelationPath[S, T](
        None, (), triple.relation_prefix
    )
    highlighting = hyperscript(highlight_triple(triple))
    match triple:
        case BasicTriple() | CopyTriple():
            inner_html = "⟶"
        case ParallelTriple(ParallelRelation(children)):
            inner_html = "".join(
                arrows_div(
                    triple.at_child(ParallelChildIndex(i))
                )
                for i, (child, _) in enumerate(children)
                # Shouldn't happen as should be expanded
                if not assert_isinstance(child, DeBruijn, False)
            )
        case _:
            raise NotImplementedError

    return inspect.cleandoc(
        f"""
        <div id="{relation_id}" _="{highlighting}">
            {inner_html}
        </div>
        """
    )


def arrows_html[S, T](visible: Relation[S, T]) -> str:
    return f'<div class="arrows">{arrows_div(visible)}</div>'
