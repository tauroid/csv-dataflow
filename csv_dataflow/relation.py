from abc import ABC, abstractmethod
import csv
from dataclasses import dataclass, field, replace
from functools import cache
from pathlib import Path
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Iterator,
    Literal,
    Self,
    TypeVar,
    cast,
)

from .sop import (
    SumProductNode,
    SumProductPath,
    add_value_to_path,
    sop_from_type,
    map_node_data as map_sop_node_data,
)

DeBruijn = int

T = TypeVar("T")

S = TypeVar("S")

Data = TypeVar("Data", default=None)

type Relation[S, T, Data=None] = (
    BasicRelation[S, T, Data]
    | ParallelRelation[S, T, Data]
    | SeriesRelation[S, T, Data]
)


@dataclass(frozen=True)
class RelationPath(Generic[S, T]):
    point: Literal["Source", "Target"]
    sop_path: SumProductPath[Any]
    stage: tuple[str, ...] = ()

    @classmethod
    def from_str(cls, s: str) -> Self:
        point, *list_path = s.split("/")
        assert point in ("Source", "Target")
        path = tuple(list_path)

        return cls(point, path)  # Deal with stage later

    @cache
    def flat(self) -> tuple[str, ...]:
        return (*self.stage, self.point, *self.sop_path)

    @cache
    def to_str(self, separator: str = "/") -> str:
        return separator.join(self.flat())


@dataclass(frozen=True)
class RelationBase(ABC, Generic[S, T, Data]):
    source: SumProductNode[S, Data] = field(repr=False, hash=False)
    target: SumProductNode[T, Data] = field(repr=False, hash=False)

    def at(self, path: RelationPath[S, T]) -> SumProductNode[Any, Data]:
        assert path.stage == ()
        match path.point:
            case "Source":
                return self.source.at(path.sop_path)
            case "Target":
                return self.target.at(path.sop_path)

    def replace_at(
        self, path: RelationPath[S, T], node: SumProductNode[Any, Data]
    ) -> Self:
        assert path.stage == ()
        match path.point:
            case "Source":
                return replace(self, source=self.source.replace_at(path.sop_path, node))
            case "Target":
                return replace(self, target=self.target.replace_at(path.sop_path, node))

    def replace_data_at(self, path: RelationPath[S, T], data: Data) -> Self:
        return self.replace_at(path, replace(self.at(path), data=data))


@dataclass(frozen=True)
class BasicRelation(Generic[S, T, Data], RelationBase[S, T, Data]):
    source_paths: tuple[SumProductPath[S], ...]
    target_paths: tuple[SumProductPath[T], ...]
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
class ParallelRelation(Generic[S, T, Data], RelationBase[S, T, Data]):
    children: tuple[Relation[S, T, Data], ...]
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
class SeriesRelation(Generic[S, T, Data], RelationBase[S, T, Data]):
    stages: tuple[tuple[Relation[Any, Any, Data], SumProductNode[Any]], ...]
    last_stage: Relation[Any, T, Data]


# FIXME Then go straight to "what does this partially specified
#       domain / range member go to / come from"
#       Then go to "is it a function"
#       Then recursion and partitions


def paths_from_csv_column_name(
    sop: SumProductNode[T], name: str, prefix: SumProductPath[Any] = ()
) -> Iterator[SumProductPath[T]]:
    for key, child in sop.children.items():
        new_prefix = (*prefix, key)
        if key == name:  # FIXME allow the docstring heading too
            yield new_prefix
        else:
            for path in paths_from_csv_column_name(child, name, new_prefix):
                yield path


class Source: ...


class Target: ...


End = Source | Target


def parallel_relation_from_csv(
    source_type: type[S], target_type: type[T], csv_path: Path
) -> ParallelRelation[S, T]:
    sop_s = sop_from_type(source_type)
    sop_t = sop_from_type(target_type)

    with open(csv_path) as f:
        csv_dict = csv.DictReader(f)

        relations: list[BasicRelation[S, T]] = []

        for row in csv_dict:
            source_paths: list[SumProductPath[S]] = []
            target_paths: list[SumProductPath[T]] = []

            end: End = Source()
            for name, value in row.items():
                # Blank is the separator for now
                if name.strip() == "":
                    end = Target()
                    continue

                match end:
                    case Source():
                        paths = paths_from_csv_column_name(sop_s, name)
                        for path in paths:
                            add_value_to_path(sop_s, path, value)
                            source_paths.append((*path, value))
                    case Target():
                        paths = paths_from_csv_column_name(sop_t, name)
                        for path in paths:
                            add_value_to_path(sop_t, path, value)
                            target_paths.append((*path, value))

            relations.append(
                BasicRelation(sop_s, sop_t, tuple(source_paths), tuple(target_paths))
            )

    return ParallelRelation(sop_s, sop_t, tuple(relations))


def iter_relation_paths(relation: Relation[S, T, Data]) -> Iterator[RelationPath[S, T]]:
    match relation:
        case BasicRelation(source_paths=source_paths, target_paths=target_paths):
            for path in source_paths:
                yield RelationPath("Source", path)
            for path in target_paths:
                yield RelationPath("Target", path)
        case ParallelRelation(children=children):
            for child in children:
                for path in iter_relation_paths(child):
                    yield path
        case SeriesRelation():
            raise NotImplementedError


def filter_relation(
    relation: Relation[S, T, Data], filter_paths: Collection[RelationPath[S, T]]
) -> Relation[S, T, Data] | None:
    """
    Reduces to BasicRelations connecting something in the
    subtree of at least one of `paths`
    """
    match relation:
        case BasicRelation():
            for filter_path in filter_paths:
                n = len(filter_path.sop_path)

                assert filter_path.stage == ()

                match filter_path.point:
                    case "Source":
                        relation_paths = relation.source_paths
                    case "Target":
                        relation_paths = relation.target_paths

                for path in relation_paths:
                    if path[:n] == filter_path.sop_path:
                        return relation

            return None

        case ParallelRelation(children=children):
            filtered_children = tuple(
                filtered_child
                for child in children
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


def replace_source_and_target(
    relation: Relation[S, T, Any],
    new_source: SumProductNode[S, Data],
    new_target: SumProductNode[T, Data],
) -> Relation[S, T, Data]:
    match relation:
        case BasicRelation(source_paths=source_paths, target_paths=target_paths):
            clipped_source_paths = tuple(set(
                new_source.clip_path(source_path)
                for source_path in source_paths
            ))
            clipped_target_paths = tuple(set(
                new_target.clip_path(target_path)
                for target_path in target_paths
            ))
            return BasicRelation(
                new_source,
                new_target,
                clipped_source_paths,
                clipped_target_paths
            )
        case ParallelRelation(children=children):
            return ParallelRelation(
                new_source,
                new_target,
                tuple(
                    replace_source_and_target(child, new_source, new_target)
                    for child in children
                ),
            )
        case SeriesRelation():
            raise NotImplementedError


def map_node_data(
    f: Callable[[A], B], relation: Relation[S, T, A]
) -> Relation[S, T, B]:
    match relation:
        case BasicRelation(source, target):
            return cast(
                BasicRelation[S, T, B],
                replace(
                    relation,
                    source=map_sop_node_data(f, source),
                    target=map_sop_node_data(f, target),
                ),
            )
        case ParallelRelation(source, target, children):
            return cast(
                ParallelRelation[S, T, B],
                replace(
                    relation,
                    source=map_sop_node_data(f, source),
                    target=map_sop_node_data(f, target),
                    children=tuple(map_node_data(f, child) for child in children),
                ),
            )
        case SeriesRelation():
            raise NotImplementedError
