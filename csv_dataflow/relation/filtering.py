from dataclasses import replace
from typing import Collection, TypeVar
from csv_dataflow.relation import (
    BasicRelation,
    Between,
    Copy,
    DeBruijn,
    ParallelRelation,
    Relation,
    RelationPath,
    SeriesRelation,
)


S = TypeVar("S")
T = TypeVar("T")


def filter_parallel_relation_child(
    child: tuple[Relation[S, T, bool] | DeBruijn, Between[S, T]],
    filter_paths: Collection[RelationPath[S, T]],
) -> tuple[Relation[S, T, bool] | DeBruijn, Between[S, T]]:
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
            return relation, between

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
    relation: Relation[S, T, bool],
    filter_paths: Collection[RelationPath[S, T]],
) -> Relation[S, T, bool]:
    """
    Reduces to BasicRelations connecting something in the
    subtree of at least one of the input paths

    The data bool is True if none of the children
    (recursively) had anything filtered, False if something
    was filtered
    """
    match relation:
        case BasicRelation(source, target) | Copy(
            source, target
        ):
            source_paths = tuple(
                map(
                    lambda p: p.sop_path,
                    filter(
                        lambda p: p.point == "Source",
                        filter_paths,
                    ),
                )
            )

            if source and source.filter_to_paths(source_paths):
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

            if target and target.filter_to_paths(target_paths):
                return relation

            return BasicRelation[S, T, bool](None, None, False)

        case ParallelRelation(children=children):
            filtered_children = tuple(
                filtered_child
                for child in children
                for filtered_child in (
                    filter_parallel_relation_child(
                        child, filter_paths
                    ),
                )
            )

            return replace(
                relation,
                children=filtered_children,
                data=all(
                    (
                        child.data
                        if not isinstance(child, DeBruijn)
                        else True
                    )
                    for child, _ in filtered_children
                ),
            )

        case SeriesRelation():
            raise NotImplementedError
