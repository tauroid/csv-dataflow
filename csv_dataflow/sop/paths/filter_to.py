from typing import Any, Collection, TypeVar

from frozendict import frozendict
from csv_dataflow.cons import Cons, ConsList, at_index
from csv_dataflow.sop import (
    DeBruijn,
    SumProductChild,
    SumProductNode,
    SumProductPath,
)


T = TypeVar("T")
Data = TypeVar("Data", default=None)


def filter_to_paths(
    node: SumProductNode[T, Data] | DeBruijn,
    paths: Collection[SumProductPath[T]],
    prev_stack: ConsList[SumProductNode[Any, Data]] = None,
) -> SumProductNode[T, Data] | None:
    """These paths have to be anchored at the root"""
    if not paths:
        return None

    if isinstance(node, int):
        node = at_index(prev_stack, node)

    if () in paths:
        return node

    children = frozendict[str, SumProductChild[Data]](
        {
            child_path: filtered_child
            for child_path, child in node.children.items()
            for filtered_child in (
                filter_to_paths(
                    child,
                    tuple(
                        path[1:]
                        for path in paths
                        if path[0] == child_path
                    ),
                    Cons(node, prev_stack),
                ),
            )
            if filtered_child is not None
        }
    )

    if not children:
        return None

    return SumProductNode(
        node.sop,
        children,
        node.data,
    )
