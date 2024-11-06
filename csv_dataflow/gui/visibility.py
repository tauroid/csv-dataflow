from typing import TypeVar

from ..sop import SumProductNode

S = TypeVar("S")
T = TypeVar("T")


def combine_same(a: T, b: T) -> T:
    assert a == b
    return a


def compute_visible_sop(
    selected: SumProductNode[T, bool],
    expanded: SumProductNode[T, bool],
    parent_expanded: bool = True,
) -> SumProductNode[T, bool] | None:
    visible_children = {
        path: child
        for path in map(combine_same, selected.children, expanded.children)
        for child in (
            compute_visible_sop(
                selected.children[path], expanded.children[path], expanded.data
            ),
        )
        if child is not None
    }
    node = SumProductNode[T, bool](
        combine_same(selected.sop, expanded.sop),
        visible_children,
        selected.data or any(child.data for child in visible_children.values()),
    )

    descendant_is_selected = node.data

    if not descendant_is_selected and not parent_expanded:
        return None

    if len(node.children) == 1:
        path, child = next(iter(node.children.items()))

        if node.sop == child.sop:
            return SumProductNode(
                node.sop,
                {
                    f"{path} / {grandchild_path}": grandchild
                    for grandchild_path, grandchild in child.children.items()
                },
            )

    return node
