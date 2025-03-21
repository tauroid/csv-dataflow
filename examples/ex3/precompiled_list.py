from frozendict import frozendict
from csv_dataflow.sop import (
    UNIT,
    SumProductChild,
    SumProductNode,
)
from csv_dataflow.relation import (
    BasicRelation,
    Between,
    ParallelRelation,
)

boolean = SumProductNode[bool](
    "+",
    frozendict[str, SumProductChild](
        {"true": UNIT, "false": UNIT}
    ),
)

flip = ParallelRelation[bool, bool](
    (
        (
            BasicRelation(UNIT, UNIT),
            Between(("true",), ("false",)),
        ),
        (
            BasicRelation(UNIT, UNIT),
            Between(("false",), ("true",)),
        ),
    )
)

sop = SumProductNode[list[bool]](
    "+",
    frozendict[str, SumProductChild](
        {
            "empty": UNIT,
            "list": SumProductNode(
                "*",
                frozendict[str, SumProductChild](
                    {"head": boolean, "tail": 1}
                ),
            ),
        }
    ),
)

relation = ParallelRelation[list[bool], list[bool]](
    (
        (
            BasicRelation(UNIT, UNIT),
            Between(("empty",), ("empty",)),
        ),
        (flip, Between(("list", "head"), ("list", "head"))),
        (
            BasicRelation(UNIT, UNIT),
            Between(("list", "tail"), ("list", "tail")),
        ),
    )
)
