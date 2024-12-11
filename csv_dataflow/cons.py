from dataclasses import dataclass
from typing import Generic, TypeVar


T = TypeVar("T")

type ConsList[T] = Cons[T] | None

@dataclass(frozen=True)
class Cons(Generic[T]):
    head: T
    tail: ConsList[T]

def at_index(l: ConsList[T], i: int) -> T:
    assert l
    if i == 0:
        return l.head
    else:
        assert i > 0
        return at_index(l.tail, i-1)
