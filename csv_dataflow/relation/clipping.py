from typing import Any, TypeVar
from csv_dataflow.cons import Cons, ConsList, at_index
from csv_dataflow.relation import (
    BasicRelation,
    Between,
    ParallelRelation,
    Relation,
    SeriesRelation,
)
from csv_dataflow.relation.recursion import assert_subpaths_if_recursive
from csv_dataflow.sop import SumProductNode, SumProductPath, clip_sop

S = TypeVar("S")
T = TypeVar("T")


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
