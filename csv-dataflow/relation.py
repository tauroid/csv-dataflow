from dataclasses import dataclass
from typing import Any, Literal, Optional, Union

SumOrProduct = Literal["+", "*"]

DeBruijn = int


@dataclass(frozen=True)
class RelatedTo:
    target: int
    """
    The number of the target node in the currently expanded target SOP
    """
    by: Optional[Union["Relation", DeBruijn]]
    """
    Either it's just related to the node at the index (under the parent relation),
    or there's more detail under the given `Relation` or the parent one denoted by
    the de Bruijn index
    """


@dataclass(frozen=True)
class SOP:
    sop: SumOrProduct
    n: int
    """Number of children, recursively"""
    children: tuple["SOP", ...]
    related_backward: tuple[RelatedTo, ...]
    related_forward: tuple[RelatedTo, ...]


@dataclass(frozen=True)
class Relation:
    stages: tuple[SOP, ...]
    """Actually multiple composed relations"""


SOPPath = tuple[str, ...]


def relation_from_types_and_linked_paths(
    s: type[Any],
    t: type[Any],
    linked_paths: tuple[tuple[tuple[SOPPath, ...], tuple[SOPPath, ...]], ...],
) -> Relation:
    # Do basic csv non recursive thing here
    #  - Build initial SOPs from types
    #  - For each path group build their leaf nodes (maybe
    #    more than one node per path) then link them
    # Then step back and just do dataflow thing on that
    # Then make sure it's a function
    # Then you can get crazy with recursion and partitions
    raise NotImplementedError
