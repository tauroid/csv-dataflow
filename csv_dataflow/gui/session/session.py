from typing import Generic, TypeVar

from flask.sessions import SessionMixin
from csv_dataflow.gui.session.user_state import TripleUserState
from csv_dataflow.relation import Triple

S = TypeVar("S")
T = TypeVar("T")

class TripleVisibility(Generic[S,T]):
    pass



class TripleSession(Generic[S, T]):
    user_state: TripleUserState[S, T]
    visible: TripleVisibility[S, T]

    def __init__(self, session: SessionMixin, triple: Triple[S, T]) -> None:
        pass
