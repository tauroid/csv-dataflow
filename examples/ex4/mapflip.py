from csv_dataflow.relation import BasicRelation, Between, ParallelRelation
from csv_dataflow.sop import UNIT
from examples.ex3.precompiled_list import sop, flip

relation = ParallelRelation[list[bool], list[bool]](
    (
        (BasicRelation(UNIT, UNIT), Between(("empty",), ("empty",))),
        (flip, Between(("list", "head"), ("list", "head"))),
        (0, Between(("list", "tail"), ("list", "tail"))),
    )
)

__all__ = ["sop"]
