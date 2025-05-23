from __future__ import annotations
from dataclasses import dataclass, replace
from functools import cache
from itertools import chain
from typing import (
    Any,
    Callable,
    Literal,
    Self,
    TypeVar,
    cast,
)


from ..newtype import NewType
from ..sop import (
    SumProductNode,
    SumProductPath,
)

DeBruijn = int

S = TypeVar("S")
T = TypeVar("T")

type Relation[S, T, Data = None] = (
    BasicRelation[S, T, Data]
    | Copy[S, T, Data]
    | ParallelRelation[S, T, Data]
    | SeriesRelation[S, T, Data]
)


class StageIndex(NewType[int]): ...


class ParallelChildIndex(NewType[int]): ...


RelationPathElement = StageIndex | ParallelChildIndex
RelationPrefix = tuple[RelationPathElement, ...]


@dataclass(frozen=True)
class RelationPath[S, T]:
    point: Literal["Source", "Target"] | None
    sop_path: SumProductPath[Any]
    relation_prefix: RelationPrefix = ()

    @classmethod
    def from_str(cls, s: str) -> Self:
        point, *list_path = s.split("/")
        assert point in ("Source", "Target")
        path = tuple(list_path)

        return cls(point, path)  # Deal with stage later

    @cache
    def flat(self) -> tuple[RelationPathElement | str, ...]:
        return (
            *self.relation_prefix,
            self.point,
            *self.sop_path,
        )

    @cache
    def to_str(self, separator: str = "/") -> str:
        return separator.join(map(str, self.flat()))

    @property
    @cache
    def as_url_path(self) -> str:
        return "/".join(map(str, self.flat()))

    @property
    @cache
    def as_id(self) -> str:
        if self.point:
            return ":".join((self.point, *self.sop_path))
        else:
            return ":".join(
                ("Relation", *map(str, self.relation_prefix))
            )

    def add_prefixes(
        self,
        relation_prefix: RelationPrefix = (),
        source_prefix: SumProductPath[S] = (),
        target_prefix: SumProductPath[T] = (),
    ) -> Self:
        new_relation_prefix = (
            *relation_prefix,
            *self.relation_prefix,
        )

        match self.point:
            case "Source":
                sop_path = (*source_prefix, *self.sop_path)
            case "Target":
                sop_path = (*target_prefix, *self.sop_path)

        return replace(
            self,
            relation_prefix=new_relation_prefix,
            sop_path=sop_path,
        )

    def subtract_prefixes(
        self,
        relation_prefix: RelationPrefix = (),
        source_prefix: SumProductPath[S] = (),
        target_prefix: SumProductPath[T] = (),
    ) -> Self:
        assert (
            self.relation_prefix[: len(relation_prefix)]
            == relation_prefix
        )

        match self.point:
            case "Source":
                sop_prefix = source_prefix
            case "Target":
                sop_prefix = target_prefix

        assert self.sop_path[: len(sop_prefix)] == sop_prefix

        return replace(
            self,
            relation_prefix=self.relation_prefix[
                len(relation_prefix) :
            ],
            sop_path=self.sop_path[len(sop_prefix) :],
        )


@dataclass(frozen=True)
class LeafRelation[S, T, Data]:
    source: SumProductNode[S, Data] | None
    target: SumProductNode[T, Data] | None
    data: Data = cast(Data, None)

    def map_data[OtherData](
        self, f: Callable[[Data], OtherData]
    ) -> BasicRelation[S, T, OtherData]:
        return BasicRelation(
            self.source.map_data(f) if self.source else None,
            self.target.map_data(f) if self.target else None,
            f(self.data),
        )


@dataclass(frozen=True)
class BasicRelation[S, T, Data = None](LeafRelation[S, T, Data]):
    """
    This is a binary relation, between all path groupings that
    "fill" the source or target path sets in the sense that for a
    particular grouping, everything in the relevant path set must
    either be in the grouping, or be on a path that is an
    alternative to something that _is_ in the grouping.

    So the relation of a link is { (g,h) | g ∈ G, h ∈ H }, for
    the set G of all full groupings on the left side, and the set
    H of all full groupings on the right side.

    This is complicated somewhat on purpose, because then I don't
    need to reprocess wacked out CSVs into something sensible,
    and also can give them meaning (even if that meaning doesn't
    turn out to be a function).

    Allowing `source` and `target` to be None mainly because
    I don't want to make a separate FilteredXRelation set of
    classes
    """

    def at(
        self, path: RelationPath[S, T]
    ) -> SumProductNode[Any, Data]:
        return at(self, path)


@dataclass(frozen=True)
class Copy[S, T, Data = None](LeafRelation[S, T, Data]):
    """
    Individually relates every leaf (and closure under "*" of
    leaves) under each leaf of `source`, in the full source type,
    to its counterpart under each other leaf of `source`, and
    each leaf of `target`, in the full target type

    This means that all the selected source branches must be the
    same, and all the selected target branches must have at least
    have this source branch as a subtree (all paths in source
    branch exist in target branches)

    The semantics as a function are a bit weird - basically "if
    the source values are the same, copy that value to all the
    target locations"

    This works differently from a BasicRelation because in that case
    the relationship between leaves of selected branches is just
    undefined (if we're even going to allow selecting not-leaves in
    BasicRelations)
    """

    def at(
        self, path: RelationPath[S, T]
    ) -> SumProductNode[Any, Data]:
        return at(self, path)


@dataclass(frozen=True)
class Between[S, T]:
    source: SumProductPath[S]
    target: SumProductPath[T]

    def subtract_from(
        self, path: RelationPath[S, T]
    ) -> RelationPath[S, T] | None:
        prefix = (
            self.source
            if path.point == "Source"
            else self.target
        )
        prefix_len = len(prefix)
        if path.sop_path[:prefix_len] == prefix:
            return replace(
                path, sop_path=path.sop_path[prefix_len:]
            )
        else:
            return None


@dataclass(frozen=True)
class ParallelRelation[S, T, Data = None]:
    children: tuple[
        tuple[
            Relation[Any, Any, Data] | DeBruijn, Between[S, T]
        ],
        ...,
    ]
    """
    They are independently satisfiable, i.e. this is the union of the
    child relations plus any (s1 ∪ s2, t1 ∪ t2) for any (s1,t1), (s2,t2)
    coming from child relations (the s1 ∪ s2, t1 ∪ t2 need not fill any
    union of child path sets, although they will fill the union of the
    ones they came from (because every member of each path set is
    already satisfied)).
    """
    data: Data = cast(Data, None)

    def at(
        self, path: RelationPath[S, T]
    ) -> SumProductNode[Any, Data]:
        return at(self, path)

    def map_data[OtherData](
        self, f: Callable[[Data], OtherData]
    ) -> ParallelRelation[S, T, OtherData]:
        return ParallelRelation(
            tuple(
                (
                    (
                        child.map_data(f)
                        if not isinstance(child, DeBruijn)
                        else child
                    ),
                    between,
                )
                for child, between in self.children
            ),
            f(self.data),
        )


@dataclass(frozen=True)
class SeriesRelation[S, T, Data = None]:
    stages: tuple[
        tuple[
            Relation[Any, Any, Data], SumProductNode[Any, Data]
        ],
        ...,
    ]
    last_stage: Relation[Any, T, Data]
    data: Data = cast(Data, None)

    def at(
        self, path: RelationPath[S, T]
    ) -> SumProductNode[Any, Data]:
        return at(self, path)

    def map_data[OtherData](
        self, f: Callable[[Data], OtherData]
    ) -> SeriesRelation[S, T, OtherData]: ...


from csv_dataflow.relation.at import at


# FIXME Then go straight to "what does this partially specified
#       domain / range member go to / come from"
#       Then go to "is it a function"
#       Then recursion and partitions
