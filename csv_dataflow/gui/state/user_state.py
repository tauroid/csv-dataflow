from __future__ import annotations
from dataclasses import dataclass, replace
from typing import Generic, TypeVar

from csv_dataflow.gui.state.pickler import (
    field_pickler,
    pickler,
)
from csv_dataflow.relation import Relation, Triple
from csv_dataflow.sop import SumProductNode


S = TypeVar("S")
T = TypeVar("T")


@pickler
@dataclass
class SOPUserState(Generic[T]):
    selected: SumProductNode[T, bool] = field_pickler()
    expanded: SumProductNode[T, bool] = field_pickler()

    @classmethod
    def from_sop(cls, sop: SumProductNode[T]) -> SOPUserState[T]:
        # replace is expanding only the top level, all other
        # levels collapsed by default
        return cls(
            replace(sop.map_data(lambda _: False), data=True),
            replace(sop.map_data(lambda _: False), data=True),
        )


@pickler
@dataclass
class TripleUserState(Generic[S, T]):
    source: SOPUserState[S]
    target: SOPUserState[T]
    relation: Relation[S, T] = field_pickler()

    @classmethod
    def uninitialised(cls) -> TripleUserState[S, T]:
        return cls(SOPUserState(), SOPUserState())

    @classmethod
    def from_triple(
        cls, triple: Triple[S, T]
    ) -> TripleUserState[S, T]:
        return cls(
            SOPUserState[S].from_sop(triple.raw_source),
            SOPUserState[T].from_sop(triple.raw_target),
            triple.relation,
        )
