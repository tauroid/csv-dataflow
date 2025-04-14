from dataclasses import dataclass
from typing import Any, Collection, Literal

from csv_dataflow.relation import (
    Copy,
    Relation,
    RelationPath,
    Triple,
)
from csv_dataflow.sop import SumProductNode, SumProductPath


@dataclass(frozen=True)
class BasicContext[S, T]:
    triple: Triple[S, T]


@dataclass(frozen=True)
class CopyContext[S, T]:
    relation: Copy[S, T]
    subpath: SumProductPath[Any]


Highlighting = Literal[
    "BareSelected",
    "Related",
    "SubRelated",
    "SubRelatedSelected",
    "HasRelatedChildren",
    "Copy",
    "ChildIsCopySelected",
    "CopySelected",
    "SubCopySelected",
]

# Can I have some kind of generic traversal function that takes
# - Context data refiner function
# - Child data combiner function
# - Evaluator function taking results of both of the above
# - Child value combiner function to get final result


# Doesn't do traversal of sop, takes results of traversal and gives highlighted paths
def node_highlighting[S, T, N](
    context: BasicContext[S, T] | CopyContext[S, T] | None,
    has_related_children: SumProductNode[N, bool],
    filtered_relation: Relation[S, T],
) -> Collection[tuple[RelationPath[S, T], Highlighting]]: ...
