from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, TypeVar

from csv_dataflow.gui.session.pickler import (
    field_pickler,
    pickler,
)
from csv_dataflow.gui.session.user_state import TripleUserState
from csv_dataflow.gui.visibility import compute_visible_sop
from csv_dataflow.relation import Relation, Triple
from csv_dataflow.relation.clipping import clip_relation
from csv_dataflow.sop import SumProductNode

S = TypeVar("S")
T = TypeVar("T")


@pickler
@dataclass
class TripleVisibility(Generic[S, T]):
    source: SumProductNode[S, bool] = field_pickler()
    target: SumProductNode[T, bool] = field_pickler()
    relation: Relation[S, T] = field_pickler()

    @classmethod
    def from_user_state(
        cls, user_state: TripleUserState[S, T]
    ) -> TripleVisibility[S, T]:
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
class TripleSession(Generic[S, T]):
    user_state: TripleUserState[S, T]
    visibility: TripleVisibility[S, T]

    @classmethod
    def from_triple(
        cls, triple: Triple[S, T]
    ) -> TripleSession[S, T]:
        user_state = TripleUserState[S, T].from_triple(triple)
        return cls(
            user_state,
            TripleVisibility[S, T].from_user_state(user_state),
        )

    def recalculate_visibility(self) -> None:
        visibility = TripleVisibility[S, T].from_user_state(
            self.user_state
        )
        self.visibility.source = visibility.source
        self.visibility.target = visibility.target
        self.visibility.relation = visibility.relation
