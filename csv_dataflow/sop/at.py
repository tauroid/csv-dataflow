from __future__ import annotations
from dataclasses import replace
from typing import TYPE_CHECKING, Any, TypeVar

from frozendict import frozendict
from csv_dataflow.cons import Cons, ConsList, at_index

if TYPE_CHECKING:
    from csv_dataflow.sop import SumProductNode, SumProductPath

T = TypeVar("T")
Data = TypeVar("Data", default=None)


def at(
    sop: SumProductNode[T, Data],
    path: SumProductPath[T],
    prev_stack: ConsList[SumProductNode[Any, Data]] = None,
) -> SumProductNode[Any, Data]:
    if not path:
        return sop

    stack = Cons(sop, prev_stack)

    child = sop.children[path[0]]
    path_tail = path[1:]

    if isinstance(child, int):
        return at_index(stack, child).at(path_tail, stack)
    else:
        return child.at(path_tail, stack)


def replace_at(
    sop: SumProductNode[T, Data],
    path: SumProductPath[T],
    node: SumProductNode[Any, Data],
    prev_stack: ConsList[SumProductNode[Any, Data]] = None,
) -> SumProductNode[T, Data]:
    assert path
    if len(path) == 1:
        return replace(
            sop, children={**sop.children, path[0]: node}
        )
    else:
        stack = Cons(sop, prev_stack)

        child = sop.children[path[0]]
        if isinstance(child, int):
            unrolled_child = at_index(stack, child)
        else:
            unrolled_child = child

        return replace(
            sop,
            children=frozendict(
                {
                    **sop.children,
                    path[0]: unrolled_child.replace_at(
                        path[1:], node, stack
                    ),
                }
            ),
        )


def replace_data_at(
    sop: SumProductNode[T, Data],
    path: SumProductPath[T],
    data: Data,
) -> SumProductNode[T, Data]:
    return sop.replace_at(path, replace(sop.at(path), data=data))
