from typing import Literal, TypeVar
from csv_dataflow.relation import Relation, Triple

State = Literal[
    "InRelation",
    "BelowBasic",
    "Copy",
    "CopyUmbra",
    "SubCopy",
    "SubSubCopy",
    "Selected",
    "Umbra",
    "Not",
]

S = TypeVar("S")
T = TypeVar("T")


def mark_up_visible_triple(
    selection: Triple[S, T], visible: Triple[S, T]
) -> Triple[S, T, State]:
    # FIXME
    # Derive filtered relations from selection
    #
    # Then structurally recurse all 3 of selection, filtered and
    # visible, retaining the relevant context, and hopefully enough
    # information will be available to just return the right state
    #
    # Then can iterate over the triple to get highlighted states
    pass
