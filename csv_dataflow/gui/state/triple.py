from __future__ import annotations
from dataclasses import dataclass
from typing import Self

from csv_dataflow.gui.state.pickler import (
    field_pickler,
    pickler,
)
from csv_dataflow.gui.state.user_state import TripleUserState
from csv_dataflow.gui.visibility import compute_visible_sop
from csv_dataflow.relation import Relation
from csv_dataflow.relation.clipping import clip_relation
from csv_dataflow.relation.triple import Triple
from csv_dataflow.sop import SumProductNode


@pickler
@dataclass
class VisibleTriple[S, T]:
    source: SumProductNode[S, bool] = field_pickler()
    target: SumProductNode[T, bool] = field_pickler()
    relation: Relation[S, T] = field_pickler()

    @classmethod
    def from_user_state(
        cls, user_state: TripleUserState[S, T]
    ) -> Self:
        source = compute_visible_sop(
            user_state.source.selected,
            user_state.source.expanded,
        )
        assert source
        target = compute_visible_sop(
            user_state.target.selected,
            user_state.target.expanded,
        )
        assert target
        return cls(
            source,
            target,
            clip_relation(
                user_state.relation,
                source,
                target,
            ),
        )


@pickler
@dataclass
class TripleState[S, T]:
    user_state: TripleUserState[S, T]
    visible: VisibleTriple[S, T]

    @classmethod
    def uninitialised(cls) -> Self:
        return cls(
            TripleUserState[S, T].uninitialised(),
            VisibleTriple(),
        )

    @classmethod
    def from_triple(cls, triple: Triple[S, T]) -> Self:
        user_state = TripleUserState[S, T].from_triple(triple)
        return cls(
            user_state,
            VisibleTriple[S, T].from_user_state(user_state),
        )

    def recalculate_visible(self) -> None:
        visible = VisibleTriple[S, T].from_user_state(
            self.user_state
        )
        self.visible.source = visible.source
        self.visible.target = visible.target
        self.visible.relation = visible.relation
