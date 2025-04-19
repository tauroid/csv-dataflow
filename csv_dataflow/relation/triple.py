from dataclasses import dataclass
from typing import Any, overload

from csv_dataflow.relation import (
    BasicRelation,
    Copy,
    DeBruijn,
    ParallelChildIndex,
    ParallelRelation,
    Relation,
    RelationPrefix,
    SeriesRelation,
)
from csv_dataflow.sop import SumProductNode, SumProductPath

type Triple[S, T, Data = None] = (
    BasicTriple[S, T, Data]
    | CopyTriple[S, T, Data]
    | ParallelTriple[S, T, Data]
    | SeriesTriple[S, T, Data]
)


@dataclass(frozen=True, kw_only=True)
class TripleMinusRelation[S, T, Data = None]:
    source: SumProductNode[S, Data]
    target: SumProductNode[T, Data]

    source_prefix: SumProductPath[S] = ()
    target_prefix: SumProductPath[T] = ()
    relation_prefix: RelationPrefix = ()


@dataclass(frozen=True)
class BasicTriple[S, T, Data = None](
    TripleMinusRelation[S, T, Data]
):
    relation: BasicRelation[S, T, Data]


@dataclass(frozen=True)
class CopyTriple[S, T, Data = None](
    TripleMinusRelation[S, T, Data]
):
    relation: Copy[S, T, Data]


@dataclass(frozen=True)
class ParallelTriple[S, T, Data = None](
    TripleMinusRelation[S, T, Data]
):
    relation: ParallelRelation[S, T, Data]

    def at_child(
        self, parallel_child_index: ParallelChildIndex
    ) -> Triple[Any, Any, Data]:
        child, between = self.relation.children[
            parallel_child_index.value
        ]

        if isinstance(child, DeBruijn):
            # Probably could do this I guess
            raise NotImplementedError

        return relation_to_triple(
            child,
            self.source.at(between.source),
            self.target.at(between.target),
            self.relation_prefix + (parallel_child_index,),
            self.source_prefix + between.source,
            self.target_prefix + between.target,
        )


@dataclass(frozen=True)
class SeriesTriple[S, T, Data = None](
    TripleMinusRelation[S, T, Data]
):
    relation: SeriesRelation[S, T, Data]


@overload
def relation_to_triple[S, T, Data](
    relation: BasicRelation[S, T, Data],
    source: SumProductNode[S, Data],
    target: SumProductNode[T, Data],
    relation_prefix: RelationPrefix = (),
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
) -> BasicTriple[S, T, Data]: ...


@overload
def relation_to_triple[S, T, Data](
    relation: Copy[S, T, Data],
    source: SumProductNode[S, Data],
    target: SumProductNode[T, Data],
    relation_prefix: RelationPrefix = (),
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
) -> CopyTriple[S, T, Data]: ...


@overload
def relation_to_triple[S, T, Data](
    relation: ParallelRelation[S, T, Data],
    source: SumProductNode[S, Data],
    target: SumProductNode[T, Data],
    relation_prefix: RelationPrefix = (),
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
) -> ParallelTriple[S, T, Data]: ...


@overload
def relation_to_triple[S, T, Data](
    relation: SeriesRelation[S, T, Data],
    source: SumProductNode[S, Data],
    target: SumProductNode[T, Data],
    relation_prefix: RelationPrefix = (),
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
) -> SeriesTriple[S, T, Data]: ...


def relation_to_triple[S, T, Data](
    relation: Relation[S, T, Data],
    source: SumProductNode[S, Data],
    target: SumProductNode[T, Data],
    relation_prefix: RelationPrefix = (),
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
) -> Triple[S, T, Data]:
    match relation:
        case BasicRelation():
            return BasicTriple(
                relation,
                source=source,
                target=target,
                relation_prefix=relation_prefix,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
            )
        case Copy():
            return CopyTriple(
                relation,
                source=source,
                target=target,
                relation_prefix=relation_prefix,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
            )
        case ParallelRelation():
            return ParallelTriple(
                relation,
                source=source,
                target=target,
                relation_prefix=relation_prefix,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
            )
        case SeriesRelation():
            return SeriesTriple(
                relation,
                source=source,
                target=target,
                relation_prefix=relation_prefix,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
            )
