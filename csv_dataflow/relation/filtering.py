from dataclasses import replace
from typing import Collection, TypeVar
from csv_dataflow.relation import (
    BasicRelation,
    Between,
    DeBruijn,
    ParallelRelation,
    Relation,
    RelationPath,
    SeriesRelation,
)


S = TypeVar("S")
T = TypeVar("T")


def filter_parallel_relation_child(
    child: tuple[Relation[S, T] | DeBruijn, Between[S, T]],
    filter_paths: Collection[RelationPath[S, T]],
) -> tuple[Relation[S, T] | DeBruijn, Between[S, T]]:
    relation, between = child
    for filter_path in filter_paths:
        between_path = (
            between.source
            if filter_path.point == "Source"
            else between.target
        )
        if (
            between_path[: len(filter_path.sop_path)]
            == filter_path.sop_path
        ):
            # Then it's entirely underneath the filter path so good
            return child

    if isinstance(relation, int):
        return relation, between

    filtered_relation = filter_relation(
        relation,
        tuple(
            relative_filter_path
            for filter_path in filter_paths
            for relative_filter_path in (
                between.subtract_from(filter_path),
            )
            if relative_filter_path is not None
        ),
    )

    return filtered_relation, between


def filter_relation(
    relation: Relation[S, T],
    filter_paths: Collection[RelationPath[S, T]],
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
                    filter(
                        lambda p: p.point == "Source",
                        filter_paths,
                    ),
                )
            )

            if (
                relation.source
                and relation.source.filter_to_paths(source_paths)
            ):
                return relation

            target_paths = tuple(
                map(
                    lambda p: p.sop_path,
                    filter(
                        lambda p: p.point == "Target",
                        filter_paths,
                    ),
                )
            )

            if (
                relation.target
                and relation.target.filter_to_paths(target_paths)
            ):
                return relation

            return BasicRelation[S, T](None, None)

        case ParallelRelation(children=children):
            filtered_relation = replace(
                relation,
                children=tuple(
                    filtered_child
                    for child in children
                    for filtered_child in (
                        filter_parallel_relation_child(
                            child, filter_paths
                        ),
                    )
                ),
            )

            return filtered_relation

        case SeriesRelation():
            raise NotImplementedError
