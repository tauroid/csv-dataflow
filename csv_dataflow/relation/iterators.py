from typing import Iterator
from csv_dataflow.relation import (
    BasicRelation,
    DeBruijn,
    ParallelChildIndex,
    ParallelRelation,
    Relation,
    RelationPath,
    RelationPrefix,
    SeriesRelation,
)
from csv_dataflow.relation.triple import (
    BasicTriple,
    CopyTriple,
    ParallelTriple,
    SeriesTriple,
    Triple,
)
from csv_dataflow.sop import SumProductPath


def iter_relation_paths[S, T, Data](
    relation: Relation[S, T, Data],
    relation_prefix: RelationPrefix = (),
    source_prefix: SumProductPath[S] = (),
    target_prefix: SumProductPath[T] = (),
) -> Iterator[RelationPath[S, T]]:
    match relation:
        case BasicRelation(source=source, target=target):
            if source is not None:
                for path in source.iter_leaf_paths(
                    source_prefix
                ):
                    yield RelationPath(
                        "Source",
                        path,
                        relation_prefix=relation_prefix,
                    )
            if target is not None:
                for path in target.iter_leaf_paths(
                    target_prefix
                ):
                    yield RelationPath(
                        "Target",
                        path,
                        relation_prefix=relation_prefix,
                    )
        case ParallelRelation(children=children):
            for i, (child, between) in enumerate(children):
                assert not isinstance(
                    child, int
                ), "Flat iterating over a recursive relation is probably a mistake"
                for path in iter_relation_paths(
                    child,
                    (*relation_prefix, ParallelChildIndex(i)),
                    (*source_prefix, *between.source),
                    (*target_prefix, *between.target),
                ):
                    yield path
        case SeriesRelation():
            raise NotImplementedError


def iter_basic_triples[S, T, Data](
    triple: Triple[S, T, Data],
) -> Iterator[BasicTriple[S, T, Data] | CopyTriple[S, T, Data]]:
    match triple:
        case BasicTriple() | CopyTriple():
            yield triple
        case ParallelTriple(relation=ParallelRelation(children)):
            for i, (child, _) in enumerate(children):
                assert not isinstance(
                    child, DeBruijn
                ), "Flat iterating over a recursive relation is probably a mistake"
                for descendant in iter_basic_triples(
                    triple.at_child(ParallelChildIndex(i))
                ):
                    yield descendant
        case SeriesTriple():
            raise NotImplementedError
