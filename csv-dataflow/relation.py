from dataclasses import dataclass, field, fields, is_dataclass
import types
from typing import Any, Literal, Mapping, Optional, Union, get_args, get_origin

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
class SumProductLeaf:
    related_backward: tuple[RelatedTo, ...] = field(kw_only=True, default=())
    related_forward: tuple[RelatedTo, ...] = field(kw_only=True, default=())


@dataclass(frozen=True)
class SumProductTree(SumProductLeaf):
    """
    Because of Python being Python, children of a Sum will all
    be Products (or neither Sum nor Product), but children of
    a Product can be either (or neither)

    (because Unions are not tagged)

    We'll just pretend that's a possibility anyway
    """

    sop: SumOrProduct
    n: int
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

    children = {
        key: sop_from_type(child_type) for key, child_type in child_types.items()
    }

    n = sum(
        child.n if isinstance(child, SumProductTree) else 1
        for child in children.values()
    )

    return SumProductTree(sop, n, children)


def relation_from_types_and_linked_paths(
    s: type[Any],
    t: type[Any],
    linked_paths: tuple[
        tuple[tuple[SumProductPath, ...], tuple[SumProductPath, ...]], ...
    ],
) -> Relation:
    # Do basic csv non recursive thing here
    #  - Build initial SOPs from types
    s_sop = sop_from_type(s)
    t_sop = sop_from_type(t)

    #  - For each path group build their leaf nodes (maybe
    #    more than one node per path) then link them
    #    - For each path in the path group:
    #       - Navigate to the (has to be) leaf node belonging to
    #         the penultimate path member
    #       - Turn it into a Sum (so I guess we need to be in the parent)
    #       - Add the possibility in the final path member as a child leaf
    #         (string representation of value in key)
    #    - Recalculate n for all nodes
    # Then step back and just do dataflow thing on that
    # Then make sure it's a function
    # Then you can get crazy with recursion and partitions
    raise NotImplementedError
