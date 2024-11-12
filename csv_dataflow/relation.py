from dataclasses import dataclass, replace
from functools import cache
from typing import (
    Any,
    Collection,
    Generic,
    Iterator,
    Literal,
    Self,
    TypeVar,
)

from .newtype import NewType
from .sop import (
    SumProductNode,
    SumProductPath,
    clip_sop,
    iter_sop_paths,
    select_from_paths,
)

DeBruijn = int

T = TypeVar("T")

S = TypeVar("S")

type Relation[S, T] = (
    BasicRelation[S, T] | ParallelRelation[S, T] | SeriesRelation[S, T]
)


class StageIndex(NewType[int]): ...


class ParallelChildIndex(NewType[int]): ...


RelationPathElement = StageIndex | ParallelChildIndex


@dataclass(frozen=True)
class RelationPath(Generic[S, T]):
    point: Literal["Source", "Target"]
    sop_path: SumProductPath[Any]
    relation_prefix: tuple[RelationPathElement, ...] = ()

    @classmethod
    def from_str(cls, s: str) -> Self:
        point, *list_path = s.split("/")
        assert point in ("Source", "Target")
        path = tuple(list_path)

        return cls(point, path)  # Deal with stage later

    @cache
    def flat(self) -> tuple[RelationPathElement | str, ...]:
        return (*self.relation_prefix, self.point, *self.sop_path)

    @cache
    def to_str(self, separator: str = "/") -> str:
        return separator.join(map(str, self.flat()))


@dataclass(frozen=True)
class BasicRelation(Generic[S, T]):
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

    source: SumProductNode[S]
    target: SumProductNode[T]


@dataclass(frozen=True)
class Between(Generic[S, T]):
    source: SumProductPath[S]
    target: SumProductPath[T]


@dataclass(frozen=True)
class ParallelRelation(Generic[S, T]):
    children: tuple[tuple[Relation[Any, Any] | DeBruijn, Between[S, T]], ...]
    """
    They are independently satisfiable, i.e. this is the union of the
    child relations plus any (s1 ∪ s2, t1 ∪ t2) for any (s1,t1), (s2,t2)
    coming from child relations (the s1 ∪ s2, t1 ∪ t2 need not fill any
    union of child path sets, although they will fill the union of the
    ones they came from (because every member of each path set is
    already satisfied)).
    """


@dataclass(frozen=True)
class SeriesRelation(Generic[S, T]):
    stages: tuple[tuple[Relation[Any, Any], SumProductNode[Any]], ...]
    last_stage: Relation[Any, T]


# FIXME Then go straight to "what does this partially specified
#       domain / range member go to / come from"
#       Then go to "is it a function"
#       Then recursion and partitions


def iter_relation_paths(relation: Relation[S, T]) -> Iterator[RelationPath[S, T]]:
    match relation:
        case BasicRelation(source=source, target=target):
            for path in iter_sop_paths(source):
                yield RelationPath("Source", path)
            for path in iter_sop_paths(target):
                yield RelationPath("Target", path)
        case ParallelRelation(children=children):
            for child, _ in children:
                assert not isinstance(
                    child, int
                ), "Flat iterating over a recursive relation is probably a mistake"
                for path in iter_relation_paths(child):
                    yield path
        case SeriesRelation():
            raise NotImplementedError


def iter_basic_relations(relation: Relation[S, T]) -> Iterator[BasicRelation[S, T]]:
    match relation:
        case BasicRelation():
            yield relation
        case ParallelRelation(children=children):
            for child, _ in children:
                assert not isinstance(
                    child, int
                ), "Flat iterating over a recursive relation is probably a mistake"
                for descendant in iter_basic_relations(child):
                    yield descendant
        case SeriesRelation():
            raise NotImplementedError

def only_has_de_bruijn_indices(relation: Relation[S,T]) -> bool:
    match relation:
        case BasicRelation():
            return False
        case ParallelRelation(children):
            return all(isinstance(child, int) or only_has_de_bruijn_indices(child) for child, _ in children)
        case SeriesRelation():
            raise NotImplementedError


def max_de_bruijn_index_relative_to_current_node(relation: Relation[S,T]) -> int:
    """0 is the argument, 1 is node above, etc"""

    match relation:
        case BasicRelation():
            return 0
        case ParallelRelation(children):
            return max(
                (
                    child if isinstance(child, int)
                    else max_de_bruijn_index_relative_to_current_node(child) - 1
                )
                for child, _ in children
            )
        case SeriesRelation():
            raise NotImplementedError

def empty_recursion(relation: Relation[S, T]) -> bool:
    return (
        only_has_de_bruijn_indices(relation)
        and max_de_bruijn_index_relative_to_current_node(relation) > 1
    )

def filter_relation(
    relation: Relation[S, T], filter_paths: Collection[RelationPath[S, T]]
) -> Relation[S, T] | None:
    """
    Reduces to BasicRelations connecting something in the
    subtree of at least one of `paths`

    TODO not using Between properly yet
    """
    match relation:
        case BasicRelation():
            source_paths = tuple(
                map(
                    lambda p: p.sop_path,
                    filter(lambda p: p.point == "Source", filter_paths),
                )
            )

            if select_from_paths(relation.source, source_paths):
                return relation

            target_paths = tuple(
                map(
                    lambda p: p.sop_path,
                    filter(lambda p: p.point == "Target", filter_paths),
                )
            )

            if select_from_paths(relation.target, target_paths):
                return relation

            return None

        # FIXME something similar to select_given_csv_paths I think
        #       probably need to abstract a bit
        case ParallelRelation(children=children):
            filtered_children = tuple(
                (filtered_child, between)
                for child, between in children
                if not isinstance(child, int)
                for filtered_child in (filter_relation(child, filter_paths),)
                if filtered_child is not None and not empty_recursion(filtered_child)
            )

            if not filtered_children:
                return None
            else:
                return replace(relation, children=filtered_children)

        case SeriesRelation():
            raise NotImplementedError


A = TypeVar("A")
B = TypeVar("B")


def clip_relation(
    relation: Relation[S, T],
    source_clip: SumProductNode[S, Any],
    target_clip: SumProductNode[T, Any],
) -> Relation[S, T]:
    match relation:
        case BasicRelation(source, target):
            return BasicRelation(
                clip_sop(source, source_clip), clip_sop(target, target_clip)
            )

        case ParallelRelation(children):
            # TODO needs changing when I use Between properly
            return ParallelRelation(
                tuple(
                    {
                        (
                            (
                                clip_relation(child, source_clip, target_clip)
                                if not isinstance(child, int)
                                else child
                            ),
                            between,
                        ): None
                        for child, between in children
                    }.keys()
                )
            )
        case SeriesRelation():
            raise NotImplementedError
