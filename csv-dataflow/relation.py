import csv
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
import types
from typing import Any, Iterator, Literal, Mapping, Union, get_args, get_origin

SumOrProduct = Literal["+", "*"]

DeBruijn = int


SumProductLeaf = tuple[()]


@dataclass(frozen=True)
class SumProductTree:
    """
    Because of Python being Python, children of a Sum will all
    be Products (or neither Sum nor Product), but children of
    a Product can be either (or neither)

    (because Unions are not tagged)

    We'll just pretend that's a possibility anyway
    """

    sop: SumOrProduct
    """Number of children, recursively"""
    children: Mapping[str, "SumProductNode"]
    """The key is the path member"""


SumProductNode = Union[SumProductTree, SumProductLeaf]

SumProductPath = tuple[str, ...]


@dataclass(frozen=True)
class BasicRelation:
    source: SumProductNode
    target: SumProductNode

    source_paths: tuple[SumProductPath, ...]
    target_paths: tuple[SumProductPath, ...]
    """
    This is a binary relation, between all path groupings that "fill" the
    source or target path sets in the sense that for a particular grouping,
    everything in the relevant path set must either be in the grouping,
    or be on a path that is an alternative to something that _is_ in the
    grouping.

    So the relation of a link is { (g,h) | g ∈ G, h ∈ H }, for the set
    G of all full groupings on the left side, and the set H of all full
    groupings on the right side.

    This is complicated somewhat on purpose, because then I don't need to
    reprocess wacked out CSVs into something sensible, and also can give
    them meaning (even if that meaning doesn't turn out to be a function).
    """


@dataclass(frozen=True)
class ParallelRelation:
    source: SumProductNode
    target: SumProductNode

    children: tuple["Relation", ...]
    """
    These have to be defined between the same `source` and `target`
    as the parent.

    They are independently satisfiable, i.e. this is the union of the
    child relations plus any (g ∪ m, h ∪ n) for any (g,h), (m,n) coming
    from child relations (the g ∪ m, h ∪ n need not fill any union of
    child path sets, although they will fill the union of the ones they
    came from (because every member of each path set is already satisfied)).
    """


@dataclass(frozen=True)
class SeriesRelation:
    source: SumProductNode
    stages: tuple[tuple["Relation", SumProductNode], ...]


Relation = Union[BasicRelation, ParallelRelation, SeriesRelation]

# FIXME Remove relation stuff from SOP and use () for blank node
#       Then go straight to "what does this partially specified
#       domain / range member go to / come from"
#       Then go to "is it a function"
#       Then recursion and partitions


def sop_from_type(t: type[Any]) -> SumProductNode:
    sop: SumOrProduct
    child_types: dict[str, Any]
    if get_origin(t) is types.UnionType:
        sop = "+"
        child_types = {child_type.__name__: child_type for child_type in get_args(t)}
    elif is_dataclass(t):
        sop = "*"
        child_types = {field.name: field.type for field in fields(t)}
    else:
        # Later more
        return SumProductLeaf()

    return SumProductTree(
        sop, {key: sop_from_type(child_type) for key, child_type in child_types.items()}
    )

def paths_from_csv_column_name(sop: SumProductNode, name: str) -> Iterator[SumProductPath]:
    raise NotImplementedError

def parallel_relation_from_csv(csv_path: Path) -> ParallelRelation:
    with open(csv_path) as f:
        csv_dict = csv.DictReader(f)

    for row in csv_dict:
        raise NotImplementedError
