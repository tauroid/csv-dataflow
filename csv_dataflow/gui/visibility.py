from typing import TypeVar

from frozendict import frozendict

from ..cons import Cons, ConsList, at_index
from ..sop import DeBruijn, SumProductChild, SumProductNode

S = TypeVar("S")
T = TypeVar("T")


def combine_same(a: T, b: T) -> T:
    assert a == b
    return a


def compute_visible_sop(
    selected: SumProductNode[T, bool] | DeBruijn,
    expanded: SumProductNode[T, bool] | DeBruijn,
    parent_expanded: bool = True,
    selected_prev_stack: ConsList[SumProductNode[T, bool]] = None,
    expanded_prev_stack: ConsList[SumProductNode[T, bool]] = None,
) -> SumProductNode[T, bool] | None:
    if isinstance(selected, DeBruijn):
        selected = at_index(selected_prev_stack, selected)
    if isinstance(expanded, DeBruijn):
        expanded = at_index(expanded_prev_stack, expanded)

    visible_children: dict[str, SumProductChild[bool]] = {
        path: child
        for path in map(combine_same, selected.children, expanded.children)
        for child in (
            compute_visible_sop(
                selected.children[path],
                expanded.children[path],
                expanded.data,
                Cons(selected, selected_prev_stack),
                Cons(expanded, expanded_prev_stack),
            ),
        )
        if child is not None
    }

    node = SumProductNode[T, bool](
        combine_same(selected.sop, expanded.sop),
        frozendict[str, SumProductChild[bool]](visible_children),
        selected.data
        or any(
            isinstance(child, SumProductNode) and child.data
            for child in visible_children.values()
        ),
    )

    descendant_is_selected = node.data

    if not descendant_is_selected and not parent_expanded:
        return None

    if len(node.children) == 1:
        path, child = next(iter(node.children.items()))

        # DeBruijn indices can't have visible children - they get
        # unrolled when selected or expanded
        if (
            isinstance(child, SumProductNode)
            and child.children
            and node.sop == child.sop
        ):
            return SumProductNode(
                node.sop,
                {
                    f"{path} / {grandchild_path}": grandchild
                    for grandchild_path, grandchild in child.children.items()
                },
            )

    return node
