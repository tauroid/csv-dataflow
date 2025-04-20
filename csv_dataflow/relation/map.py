from dataclasses import replace
from typing import Callable, cast
from csv_dataflow.relation import (
    BasicRelation,
    Copy,
    DeBruijn,
    ParallelRelation,
    Relation,
    SeriesRelation,
)


def map_relation_data[S, T, A, B](
    f: Callable[[A], B], relation: Relation[S, T, A]
) -> Relation[S, T, B]:
    match relation:
        case BasicRelation() | Copy():
            return cast(
                Relation[S, T, B],
                replace(relation, data=f(relation.data)),
            )
        case ParallelRelation(children, data):
            return cast(
                Relation[S, T, B],
                replace(
                    relation,
                    children=tuple(
                        (
                            (
                                map_relation_data(f, child),
                                between,
                            )
                            if not isinstance(child, DeBruijn)
                            else child
                        )
                        for child, between in children
                    ),
                    data=f(data),
                ),
            )
        case SeriesRelation():
            raise NotImplementedError
