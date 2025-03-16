from dataclasses import replace
import pickle
from typing import Generic, MutableMapping, TypeVar, cast

from flask.sessions import SessionMixin

from csv_dataflow.relation import Relation, Triple
from csv_dataflow.sop import SumProductNode, map_node_data


S = TypeVar("S")
T = TypeVar("T")


class SOPUserState(Generic[T]):
    _selected: SumProductNode[T, bool]
    _expanded: SumProductNode[T, bool]


class TripleUserState(Generic[S, T]):
    _session: SessionMixin
    _triple_name: str

    _source: SOPUserState[S]

    @property
    def source(self) -> SOPUserState[S]:
        return self._source

    _target: SOPUserState[T]

    @property
    def target(self) -> SOPUserState[T]:
        return self._target

    _relation: Relation[S, T] | None = None

    @property
    def _relation_key(self) -> str:
        return f"{self._triple_name}_relation"

    @property
    def relation(self) -> Relation[S, T]:
        if self._relation is None:
            self._relation = cast(Relation[S,T], pickle.loads(self._session[self._relation_key]))

        return self._relation

    @relation.setter
    def relation(self, relation: Relation[S,T]) -> None:
        self._relation = relation
        self._session[self._relation_key] = pickle.dumps(relation)

    def __init__(self, session: SessionMixin, triple: Triple[S, T]) -> None:
        try:
            self.relation
        except KeyError:
            self.relation = triple.relation


def load_relation_user_state(
    session: SessionMixin, name: str, triple: Triple[S, T]
) -> TripleUserState[S, T]:
    typed_session = cast(MutableMapping[str, bytes], session)
    if not typed_session.get(f"{name}_relation"):
        relation = triple.relation
        typed_session[f"{name}_relation"] = pickle.dumps(relation)

        source_selected = map_node_data(lambda _: False, triple.source)
        target_selected = map_node_data(lambda _: False, triple.target)
        typed_session[f"{name}_source_selected"] = pickle.dumps(source_selected)
        typed_session[f"{name}_target_selected"] = pickle.dumps(target_selected)

        source_expanded = map_node_data(lambda _: False, triple.source)
        target_expanded = map_node_data(lambda _: False, triple.target)
        # Expand top level
        source_expanded = replace(source_expanded, data=True)
        target_expanded = replace(target_expanded, data=True)
        typed_session[f"{name}_source_expanded"] = pickle.dumps(source_expanded)
        typed_session[f"{name}_target_expanded"] = pickle.dumps(target_expanded)
    else:
        relation = pickle.loads(typed_session[f"{name}_relation"])
        source_selected = pickle.loads(typed_session[f"{name}_source_selected"])
        target_selected = pickle.loads(typed_session[f"{name}_target_selected"])
        source_expanded = pickle.loads(typed_session[f"{name}_source_expanded"])
        target_expanded = pickle.loads(typed_session[f"{name}_target_expanded"])

    return TripleUserState(
        SOPUserState(source_selected, source_expanded),
        SOPUserState(target_selected, target_expanded),
        relation,
    )
