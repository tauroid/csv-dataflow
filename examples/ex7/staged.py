# I know there's a more complicated one in ex2 but let's start somewhere tractable

from dataclasses import dataclass, replace
from typing import Generic, TypeVar
from csv_dataflow.decompose import decompose

T = TypeVar("T")

@dataclass(frozen=True)
class Cage(Generic[T]):
    bird: T

@dataclass(frozen=True)
class Finch:
    clean: bool

@dataclass(frozen=True)
class Eagle:
    pedicured: bool

@dataclass(frozen=True)
class X:
    x: bool
@dataclass(frozen=True)
class Y:
    y: bool

@decompose
def uncage(caged: Cage[T]) -> T:
    return caged.bird

@decompose
def cage(bird: T) -> Cage[T]:
    return Cage(bird)


@decompose
def bathe(finch: Finch) -> Finch:
    return replace(finch, clean=True)

@decompose
def pedicure(eagle: Eagle) -> Eagle:
    return replace(eagle, pedicured=True)


@decompose
def f(caged: Cage[Finch | Eagle]) -> Cage[Finch | Eagle]:
    bird = uncage(caged)

    match bird:
        case Finch():
            bird = bathe(bird)
        case Eagle():
            bird = pedicure(bird)

    return cage(bird)
