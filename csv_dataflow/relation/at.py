from dataclasses import replace
from typing import Any, TypeVar

from csv_dataflow.relation import (
    BasicRelation,
    DeBruijn,
    ParallelRelation,
    Relation,
    RelationPath,
    SeriesRelation,
)
from csv_dataflow.sop import SumProductNode


S = TypeVar("S")
T = TypeVar("T")


def path_relation_mismatch_msg(cls_name: str) -> str:
    return (
        "relation_prefix corresponded to a"
        f" {cls_name}, so there are no SOPs to"
        " continue down. The path doesn't match the"
        " relation"
    )


def at(
    relation: Relation[S, T], path: RelationPath[S, T]
) -> SumProductNode[Any]:
    if not path.relation_prefix:
        match relation:
            case BasicRelation(source, target):
                match path.point:
                    case "Source":
                        assert source
                        return source.at(path.sop_path)
                    case "Target":
                        assert target
                        return target.at(path.sop_path)
            case ParallelRelation():
                raise ValueError(
                    path_relation_mismatch_msg(
                        "ParallelRelation"
                    )
                )
            case SeriesRelation():
                raise ValueError(
                    path_relation_mismatch_msg("SeriesRelation")
                )

    assert isinstance(relation, ParallelRelation)

    child_index, *rest_of_prefix = path.relation_prefix
    child, between = relation.children[child_index.value]

    # NOTE do this when you need it
    assert not isinstance(child, DeBruijn)

    match path.point:
        case "Source":
            sop_prefix = between.source
        case "Target":
            sop_prefix = between.target

    assert path.sop_path[: len(sop_prefix)] == sop_prefix

    sop_path = path.sop_path[len(sop_prefix) :]

    return at(
        child,
        replace(
            path,
            sop_path=sop_path,
            relation_prefix=rest_of_prefix,
        ),
    )
