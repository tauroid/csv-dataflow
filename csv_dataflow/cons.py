from dataclasses import dataclass
from typing import Generic, Iterable, Iterator, TypeVar

T = TypeVar("T", covariant=True)

type ConsList[T] = Cons[T] | None


@dataclass(frozen=True)
class Cons(Generic[T]):
    head: T
    tail: ConsList[T]


def at_index[T](l: ConsList[T], i: int) -> T:
    assert l
    if i == 0:
        return l.head
    else:
        assert i > 0
        return at_index(l.tail, i - 1)


def to_cons_list[T](it: Iterable[T]) -> ConsList[T]:
    try:
        item = next(iter(it))
        return Cons(item, to_cons_list(it))
    except StopIteration:
        return None


def iter_cons_list[T](l: ConsList[T]) -> Iterator[T]:
    match l:
        case Cons(head, tail):
            yield head
            return iter_cons_list(tail)
        case None:
            return
