from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class A:
    name: str
    """Name"""
    option: bool
    slots: tuple[int, ...]


@dataclass(frozen=True)
class B:
    @dataclass(frozen=True)
    class Code:
        x: int
        """The X thing"""
        y: int
        """The Y thing"""
        z: int
        """The Z thing"""

    @dataclass(frozen=True)
    class Option1:
        """Option 1"""
        m: bool
        n: bool
        o: str

    @dataclass(frozen=True)
    class Option2:
        """Option 2"""
        m: bool
        n: bool
        p: str

    code: Code
    deets: Option1 | Option2
    slots: tuple[int, ...]


@dataclass(frozen=True)
class C:
    name: str
    slots: tuple[int, ...]
    deets: str


def fn_from_csvs[
    S, T
](from_type: S, to_type: T, csvs: tuple[Path, ...]) -> Callable[[S], T]:
    raise NotImplementedError

