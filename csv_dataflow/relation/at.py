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

    child_index, *_ = path.relation_prefix
    child, between = relation.children[child_index.value]

    # NOTE do this when you need it
    assert not isinstance(child, DeBruijn)

    return at(
        child,
        replace(
            path.subtract_prefixes(
                (child_index,), between.source, between.target
            )
        ),
    )
