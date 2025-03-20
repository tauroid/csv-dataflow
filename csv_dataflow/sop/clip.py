from __future__ import annotations
from dataclasses import replace
from typing import TYPE_CHECKING, Any, TypeVar

from frozendict import frozendict

from csv_dataflow.cons import Cons, ConsList, at_index

if TYPE_CHECKING:
    from csv_dataflow.sop import (
        SumProductChild,
        SumProductNode,
        SumProductPath,
    )

T = TypeVar("T")
Data = TypeVar("Data", default=None)


def clip_path(
    sop: SumProductNode[T, Data],
    path: SumProductPath[T],
    path_prefix: SumProductPath[T] = (),
    prev_stack: ConsList[SumProductNode[Any, Data]] = None,
) -> SumProductPath[T]:
    if not path:
        return path_prefix

    child_path, path_tail = path[0], path[1:]

    child = sop.children.get(child_path)

    if not child:
        if sop.children:
            raise Exception(
                "Will only clip a path if an empty node is found"
                " along it, not if there are children but the"
                " next one on the path isn't found"
            )
        return path_prefix

    stack = Cons(sop, prev_stack)

    child_path_prefix = (*path_prefix, child_path)

    if isinstance(child, int):
        return at_index(stack, child).clip_path(
            path_tail, child_path_prefix, stack
        )
    else:
        return child.clip_path(
            path_tail, child_path_prefix, stack
        )


def clip(
    sop: SumProductNode[T, Data],
    clip_sop: SumProductNode[T, Any],
    prev_stack: ConsList[SumProductNode[Any, Data]] = None,
) -> SumProductNode[T, Data]:
    stack = Cons(sop, prev_stack)

    children_dict: dict[str, SumProductChild[Data]] = {}
    for path, clip_child in clip_sop.children.items():
        assert not isinstance(
            clip_child, int
        ), "Potentially this could make sense but we haven't needed it yet"

        sop_child = sop.children.get(path)
        if isinstance(sop_child, int):
            unrolled_sop_child = at_index(stack, sop_child)
        else:
            unrolled_sop_child = sop_child

        if unrolled_sop_child:
            children_dict[path] = clip(
                unrolled_sop_child, clip_child, stack
            )

    return replace(sop, children=frozendict(children_dict))
