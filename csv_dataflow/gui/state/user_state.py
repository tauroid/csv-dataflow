from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, TypeVar

from csv_dataflow.gui.state.pickler import (
    field_pickler,
    pickler,
)
from csv_dataflow.relation import Relation, Triple
from csv_dataflow.sop import SumProductNode, map_node_data


S = TypeVar("S")
T = TypeVar("T")


@pickler
@dataclass
class SOPUserState(Generic[T]):
    selected: SumProductNode[T, bool] = field_pickler()
    expanded: SumProductNode[T, bool] = field_pickler()

    @classmethod
    def from_sop(cls, sop: SumProductNode[T]) -> SOPUserState[T]:
        return cls(
            map_node_data(lambda _: False, sop),
            map_node_data(lambda _: False, sop),
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
            SOPUserState[S].from_sop(triple.source),
            SOPUserState[T].from_sop(triple.target),
            triple.relation,
        )
