from abc import ABC, abstractmethod
import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Generic,
    Iterator,
    TypeVar,
)

from .id_cache import id_cache
from .sop import SumProductNode, SumProductPath, add_value_to_path, sop_from_type

DeBruijn = int

T = TypeVar("T")

S = TypeVar("S")


type Relation[S, T] = (
    BasicRelation[S, T] | ParallelRelation[S, T] | SeriesRelation[S, T]
)


@dataclass(frozen=True)
class BasicRelation(Generic[S, T]):
    source: SumProductNode[S] = field(repr=False)
    target: SumProductNode[T] = field(repr=False)

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
class ParallelRelation(Generic[S, T]):
    source: SumProductNode[S] = field(repr=False)
    target: SumProductNode[T] = field(repr=False)

    children: tuple[Relation[S, T], ...]
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
class SeriesRelation(Generic[S, T]):
    source: SumProductNode[S] = field(repr=False)
    target: SumProductNode[T] = field(repr=False)

    stages: tuple[tuple[Relation[Any, Any], SumProductNode[Any]], ...]
    last_stage: Relation[Any, T]


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


type FilteredRelation[S, T] = (
    FilteredBasicRelation[S, T] | FilteredParallelRelation[S, T]
)


class IFilteredRelation(ABC):
    @abstractmethod
    # There's interior mutability in the sops but not the relations
    # so we're good with id_cache
    @id_cache
    def is_empty(self) -> bool: ...


@dataclass(frozen=True)
class FilteredBasicRelation(Generic[S, T], IFilteredRelation):
    basic_relation: BasicRelation[S, T] = field(repr=False)
    source_filter_paths: tuple[SumProductPath[S], ...] | None
    target_filter_paths: tuple[SumProductPath[T], ...] | None

    @id_cache
    def is_empty(self) -> bool:
        """
        Empty means there is no relation member containing at
        least one path in each active filter.
        """
        return (
            self.source_filter_paths is not None
            and all(
                path not in self.basic_relation.source_paths
                for path in self.source_filter_paths
            )
        ) or (
            self.target_filter_paths is not None
            and all(
                path not in self.basic_relation.target_paths
                for path in self.target_filter_paths
            )
        )


@dataclass(frozen=True)
class FilteredParallelRelation(Generic[S, T], IFilteredRelation):
    parallel_relation: ParallelRelation[S, T] = field(repr=False)
    filtered_children: tuple[FilteredRelation[S, T], ...]

    @id_cache
    def is_empty(self) -> bool:
        """
        Ideally remove empty children during construction
        unless there's a huge need to keep them around

        (you can see the original children under
         self.parallel_relation anyway)
        """
        return all(child.is_empty() for child in self.filtered_children)


def filter_relation(
    relation: Relation[S, T],
    source_paths: tuple[SumProductPath[S], ...] | None,
    target_paths: tuple[SumProductPath[T], ...] | None,
) -> FilteredRelation[S, T]:
    """
    Filter down to BasicRelations that contain at least one of
    source_paths and one of target_paths. If source_paths or
    target_paths are unspecified, that side won't filter.
    """
    match relation:
        case BasicRelation():
            return FilteredBasicRelation(relation, source_paths, target_paths)

        case ParallelRelation(children=children):
            filtered_children = (
                filter_relation(child, source_paths, target_paths) for child in children
            )
            return FilteredParallelRelation(
                relation,
                tuple(
                    filtered_child
                    for filtered_child in filtered_children
                    if not filtered_child.is_empty()
                ),
            )

        case SeriesRelation():
            raise NotImplementedError
