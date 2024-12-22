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

from csv_dataflow.cons import Cons, ConsList, at_index

from .newtype import NewType
from .sop import (
    SumProductChild,
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

    Allowing `source` and `target` to be None mainly because
    I don't want to make a separate FilteredXRelation set of
    classes
    """

    source: SumProductNode[S] | None
    target: SumProductNode[T] | None


@dataclass(frozen=True)
class Between(Generic[S, T]):
    source: SumProductPath[S]
    target: SumProductPath[T]

    def subtract_from(self, path: RelationPath[S, T]) -> RelationPath[S, T] | None:
        prefix = self.source if path.point == "Source" else self.target
        prefix_len = len(prefix)
        if path.sop_path[:prefix_len] == prefix:
            return replace(path, sop_path=path.sop_path[prefix_len:])
        else:
            return None


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


def iter_relation_paths(
    relation: Relation[S, T],
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
) -> Iterator[RelationPath[S, T]]:
    match relation:
        case BasicRelation(source=source, target=target):
            if source is not None:
                for path in iter_sop_paths(source):
                    yield RelationPath("Source", (*source_prefix, *path))
            if target is not None:
                for path in iter_sop_paths(target):
                    yield RelationPath("Target", (*target_prefix, *path))
        case ParallelRelation(children=children):
            for child, between in children:
                assert not isinstance(
                    child, int
                ), "Flat iterating over a recursive relation is probably a mistake"
                for path in iter_relation_paths(
                    child,
                    (*source_prefix, *between.source),
                    (*target_prefix, *between.target),
                ):
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


def only_has_de_bruijn_indices(relation: Relation[S, T]) -> bool:
    match relation:
        case BasicRelation():
            return False
        case ParallelRelation(children):
            return all(
                isinstance(child, int) or only_has_de_bruijn_indices(child)
                for child, _ in children
            )
        case SeriesRelation():
            raise NotImplementedError


def max_de_bruijn_index_relative_to_current_node(relation: Relation[S, T]) -> int:
    """0 is the argument, 1 is node above, etc"""

    match relation:
        case BasicRelation():
            return 0
        case ParallelRelation(children):
            return max(
                (
                    child
                    if isinstance(child, int)
                    else max_de_bruijn_index_relative_to_current_node(child) - 1
                )
                for child, _ in children
            )
        case SeriesRelation():
            raise NotImplementedError


def empty_recursion(relation: Relation[S, T]) -> bool:
    return (
        only_has_de_bruijn_indices(relation)
        and max_de_bruijn_index_relative_to_current_node(relation) <= 0
    )


def filter_parallel_relation_child(
    child: tuple[Relation[S, T] | DeBruijn, Between[S, T]],
    filter_paths: Collection[RelationPath[S, T]],
) -> tuple[Relation[S, T] | DeBruijn, Between[S, T]]:
    relation, between = child
    for filter_path in filter_paths:
        between_path = (
            between.source if filter_path.point == "Source" else between.target
        )
        if between_path[: len(filter_path.sop_path)] == filter_path.sop_path:
            # Then it's entirely underneath the filter path so good
            return child

    if isinstance(relation, int):
        return relation, between

    filtered_relation = filter_relation(
        relation,
        tuple(
            relative_filter_path
            for filter_path in filter_paths
            for relative_filter_path in (between.subtract_from(filter_path),)
            if relative_filter_path is not None
        ),
    )

    return filtered_relation, between


def filter_relation(
    relation: Relation[S, T], filter_paths: Collection[RelationPath[S, T]]
) -> Relation[S, T]:
    """
    Reduces to BasicRelations connecting something in the
    subtree of at least one of the input paths

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

            if relation.source and select_from_paths(relation.source, source_paths):
                return relation

            target_paths = tuple(
                map(
                    lambda p: p.sop_path,
                    filter(lambda p: p.point == "Target", filter_paths),
                )
            )

            if relation.target and select_from_paths(relation.target, target_paths):
                return relation

            return BasicRelation[S, T](None, None)

        case ParallelRelation(children=children):
            filtered_relation = replace(
                relation,
                children=tuple(
                    filtered_child
                    for child in children
                    for filtered_child in (
                        filter_parallel_relation_child(child, filter_paths),
                    )
                ),
            )

            return filtered_relation

        case SeriesRelation():
            raise NotImplementedError


A = TypeVar("A")
B = TypeVar("B")


def assert_subpaths_if_recursive(
    child: Relation[S, T] | DeBruijn, between: Between[S, T]
) -> bool:
    if isinstance(child, int):
        assert between.source and between.target, (
            "Recursion in relations should happen between subpaths"
            " of the source and target, so that clipping always"
            " terminates"
        )
    return True


def clip_relation(
    relation: Relation[S, T],
    source_clip: SumProductNode[S, Any],
    target_clip: SumProductNode[T, Any],
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
    prev_stack: ConsList[Relation[S, T]] = None,
) -> Relation[S, T]:
    clipped_source_prefix = source_clip.clip_path(source_prefix)
    clipped_target_prefix = target_clip.clip_path(target_prefix)
    match relation:
        case BasicRelation(source, target):
            assert source and target
            if clipped_source_prefix == source_prefix:
                source_clip = source_clip.at(source_prefix)
                source = clip_sop(source, source_clip)
            else:
                source = source_clip.at(clipped_source_prefix)

            if clipped_target_prefix == target_prefix:
                target_clip = target_clip.at(target_prefix)
                target = clip_sop(target, target_clip)
            else:
                target = target_clip.at(clipped_target_prefix)

            return BasicRelation(source, target)

        case ParallelRelation(children):
            if (
                clipped_source_prefix != source_prefix
                and clipped_target_prefix != target_prefix
            ):
                # We're below clip so just summarise
                return BasicRelation(
                    source_clip.at(clipped_source_prefix),
                    target_clip.at(clipped_target_prefix),
                )

            stack = Cons(relation, prev_stack)
            children = tuple(
                (
                    (
                        clip_relation(
                            (
                                child
                                if not isinstance(child, int)
                                else at_index(stack, child)
                            ),
                            source_clip,
                            target_clip,
                            between_source,
                            between_target,
                            stack,
                        )
                    ),
                    clipped_between,
                )
                for child, between in children
                if assert_subpaths_if_recursive(child, between)
                for between_source in ((*source_prefix, *between.source),)
                for between_target in ((*target_prefix, *between.target),)
                for clipped_between in (
                    Between[S, T](
                        source_clip.clip_path(between_source)[len(source_prefix) :],
                        target_clip.clip_path(between_target)[len(target_prefix) :],
                    ),
                )
            )
            # Flatten child ParallelRelations with one child
            children = tuple(
                (
                    (
                        child.children[0][0],
                        Between[S, T](
                            (*between.source, *child.children[0][1].source),
                            (*between.target, *child.children[0][1].target),
                        ),
                    )
                    if isinstance(child, ParallelRelation) and len(child.children) == 1
                    else (child, between)
                )
                for child, between in children
            )
            # Remove dupes
            return ParallelRelation(tuple({child: None for child in children}.keys()))
        case SeriesRelation():
            raise NotImplementedError
