from dataclasses import dataclass
from csv_dataflow.relation import Copy
from csv_dataflow.sop import UNIT, SumProductNode
from csv_dataflow.sop.from_type import sop_from_type


@dataclass(frozen=True)
class SomethingOrOther:
    a: int
    b: str
    c: tuple[bool, bool]


sop = sop_from_type(SomethingOrOther)
source = sop
target = SumProductNode[tuple[SomethingOrOther, SomethingOrOther]](
    "*", {"t0": sop, "t1": sop}
)

relation = Copy[SomethingOrOther, tuple[SomethingOrOther, SomethingOrOther]](
    UNIT,
    SumProductNode[tuple[SomethingOrOther, SomethingOrOther]](
        "*", {"t0": UNIT, "t1": UNIT}
    ),
)
