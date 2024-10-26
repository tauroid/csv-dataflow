import csv
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
import types
from typing import (
    Any,
    Generic,
    Iterator,
    Literal,
    Mapping,
    MutableMapping,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    overload,
)

SumOrProduct = Literal["+", "*"]

DeBruijn = int

T = TypeVar("T")


@dataclass(frozen=True)
class SumProductPath(Generic[T]):
    _path: tuple[str, ...]

    def __iter__(self):
        return iter(self._path)

    def __len__(self):
        return len(self._path)

    @overload
    def __getitem__(self, the_slice: int) -> str: ...

    @overload
    def __getitem__(self, the_slice: slice) -> "SumProductPath[T]": ...

    def __getitem__(self, the_slice: int | slice) -> "str | SumProductPath[T]":
        if isinstance(the_slice, int):
            return self._path[the_slice]
        else:
            return SumProductPath(self._path[the_slice])


@dataclass(frozen=True)
class SumProductNode(Generic[T]):
    """
    Because of Python being Python, children of a Sum will all
    be Products (or neither Sum nor Product), but children of
    a Product can be either (or neither)

    (because Unions are not tagged)

    We'll just pretend that's a possibility anyway
    """

    sop: SumOrProduct
    """Number of children, recursively"""
    children: Mapping[str, "SumProductNode[T]"]
    """The key is the path member"""

    def at(self, path: SumProductPath[T]) -> "SumProductNode[Any]":
        raise NotImplementedError


S = TypeVar("S")


@dataclass(frozen=True)
class BasicRelation(Generic[S, T]):
    source: SumProductNode[S]
    target: SumProductNode[T]

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
    source: SumProductNode[S]
    target: SumProductNode[T]

    children: tuple["Relation[S,T]", ...]
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
    source: SumProductNode[S]
    target: SumProductNode[T]

    stages: tuple[tuple["Relation[Any,Any]", SumProductNode[Any]], ...]
    last_stage: "Relation[Any, T]"


Relation = Union[BasicRelation[S, T], ParallelRelation[S, T], SeriesRelation[S, T]]

# FIXME Remove relation stuff from SOP and use () for blank node
#       Then go straight to "what does this partially specified
#       domain / range member go to / come from"
#       Then go to "is it a function"
#       Then recursion and partitions


def sop_from_type(t: type[T]) -> SumProductNode[T]:
    sop: SumOrProduct
    child_types: dict[str, Any]
    if get_origin(t) is types.UnionType:
        sop = "+"
        child_types = {child_type.__name__: child_type for child_type in get_args(t)}
    elif is_dataclass(t):
        sop = "*"
        child_types = {field.name: field.type for field in fields(t)}
    else:
        # Assume remaining types are primitive i.e. sum
        # Not actually true, will want to think about lists etc
        sop = "+"
        child_types = {}

    return SumProductNode(
        sop, {key: sop_from_type(child_type) for key, child_type in child_types.items()}
    )


def paths_from_csv_column_name(
    sop: SumProductNode[T], name: str, prefix: SumProductPath[Any] = SumProductPath(())
) -> Iterator[SumProductPath[T]]:
    for key, child in sop.children.items():
        new_prefix = SumProductPath[Any]((*prefix, key))
        if key == name:  # FIXME allow the docstring heading too
            yield new_prefix
        else:
            for path in paths_from_csv_column_name(child, name, new_prefix):
                yield path


def add_value_to_path(sop: SumProductNode[T], path: SumProductPath[T], value: str):
    if len(path) == 0:
        children = cast(MutableMapping[str, SumProductNode[Any]], sop.children)
        assert value not in children
        children[value] = SumProductNode("*", {})  # the empty product i.e. 1 :):):)
    else:
        add_value_to_path(sop.children[path[0]], path[1:], value)


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
                        source_paths.append(SumProductPath((*path, value)))
                case Target():
                    paths = paths_from_csv_column_name(sop_t, name)
                    for path in paths:
                        add_value_to_path(sop_t, path, value)
                        target_paths.append(SumProductPath((*path, value)))

        relations.append(
            BasicRelation(sop_s, sop_t, tuple(source_paths), tuple(target_paths))
        )

    return ParallelRelation(sop_s, sop_t, tuple(relations))
