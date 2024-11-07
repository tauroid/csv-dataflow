import csv
from dataclasses import dataclass, replace
from functools import cache
from itertools import chain
from pathlib import Path
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
    add_paths,
    clip_sop,
    iter_sop_paths,
    select_from_paths,
    sop_from_type,
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


def paths_from_csv_column_name(
    sop: SumProductNode[T],
    name_path: tuple[str, ...],
    prefix: SumProductPath[Any] = (),
    immediate: bool = False,
) -> Iterator[SumProductPath[T]]:
    name = name_path[0]
    something_matched = False
    for key, child in sop.children.items():
        new_prefix = (*prefix, key)
        if key == name:  # FIXME allow the docstring heading too
            something_matched = True
            if len(name_path) == 1:
                yield new_prefix
            else:
                for path in paths_from_csv_column_name(
                    child, name_path[1:], new_prefix, True
                ):
                    yield path
        else:
            if not immediate:
                for path in paths_from_csv_column_name(child, name_path, new_prefix):
                    yield path

    if immediate and not something_matched:
        raise ValueError(
            f"{name_path} didn't match any immediate child {sop.children.keys()}"
        )


class Source: ...


class Target: ...


End = Source | Target


def parallel_relation_from_csv(
    s: type[S], t: type[T], csv_path: Path
) -> tuple[SumProductNode[S], SumProductNode[T], ParallelRelation[S, T]]:
    sop_s = sop_from_type(s)
    sop_t = sop_from_type(t)

    with open(csv_path) as f:
        csv_dict = csv.DictReader(f)

        relation_paths: list[
            tuple[list[SumProductPath[S]], list[SumProductPath[T]]]
        ] = []

        for row in csv_dict:
            source_paths: list[SumProductPath[S]] = []
            target_paths: list[SumProductPath[T]] = []

            end: End = Source()
            for name, value in row.items():
                # Blank is the separator for now
                if name.strip() == "":
                    end = Target()
                    continue

                # Blank value (for now) means no connection
                if value == "":
                    continue

                match end:
                    case Source():
                        for path in paths_from_csv_column_name(
                            sop_s, tuple(map(str.strip, name.split("/")))
                        ):
                            source_paths.append((*path, value))
                    case Target():
                        for path in paths_from_csv_column_name(
                            sop_t, tuple(map(str.strip, name.split("/")))
                        ):
                            target_paths.append((*path, value))

            relation_paths.append((source_paths, target_paths))

    all_source_paths = chain.from_iterable(p[0] for p in relation_paths)
    all_target_paths = chain.from_iterable(p[1] for p in relation_paths)

    sop_s = add_paths(sop_s, all_source_paths)
    sop_t = add_paths(sop_t, all_target_paths)

    return (
        sop_s,
        sop_t,
        ParallelRelation(
            tuple(
                (
                    BasicRelation(
                        select_from_paths(sop_s, source_paths),
                        select_from_paths(sop_t, target_paths),
                    ),
                    Between((), ()),
                )
                for source_paths, target_paths in relation_paths
            )
        ),
    )


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
            for filter_path in filter_paths:
                n = len(filter_path.sop_path)

                assert filter_path.relation_prefix == ()

                match filter_path.point:
                    case "Source":
                        relation_paths = iter_sop_paths(relation.source)
                    case "Target":
                        relation_paths = iter_sop_paths(relation.target)

                for path in relation_paths:
                    if path[:n] == filter_path.sop_path:
                        return relation

            return None

        case ParallelRelation(children=children):
            filtered_children = tuple(
                (filtered_child, between)
                for child, between in children
                for filtered_child in (filter_relation(child, filter_paths),)
                if filtered_child is not None
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
                    dict(
                        (
                            (clip_relation(child, source_clip, target_clip), between),
                            None,
                        )
                        for child, between in children
                    ).keys()
                )
            )
        case SeriesRelation():
            raise NotImplementedError
