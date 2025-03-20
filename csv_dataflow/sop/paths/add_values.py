from typing import Any, Collection, TypeVar

from frozendict import frozendict
from csv_dataflow.cons import Cons, ConsList, at_index
from csv_dataflow.sop import (
    UNIT,
    DeBruijn,
    SumProductChild,
    SumProductNode,
    SumProductPath,
)


T = TypeVar("T")
Data = TypeVar("Data", default=None)


def add_values_at_paths(
    node: SumProductNode[T, Data] | DeBruijn,
    paths: Collection[SumProductPath[T]],
    prev_stack: ConsList[SumProductNode[Any, Data]] = None,
    active_paths: Collection[SumProductPath[T]] = (),
    recursing: bool = False,
) -> SumProductNode[T, Data] | DeBruijn:
    """
    Find the penultimate node of each path and add the final
    value to it as a child

    The paths don't have to be anchored at the root, any sequence
    of branches that matches works
    """
    if isinstance(node, DeBruijn):
        if recursing:
            return node

        node = at_index(prev_stack, node)
        recursing = True

    paths = (*paths, *active_paths)

    values = tuple(path[0] for path in paths if len(path) == 1)

    children = frozendict[str, SumProductChild[Data]](
        {
            **{value: UNIT for value in values},
            **{
                child_path: add_values_at_paths(
                    child,
                    paths,
                    Cons(node, prev_stack),
                    tuple(
                        path[1:]
                        for path in paths
                        if path[0] == child_path
                    ),
                    recursing,
                )
                for child_path, child in node.children.items()
            },
        }
    )

    return SumProductNode(
        node.sop,
        children,
        node.data,
    )
