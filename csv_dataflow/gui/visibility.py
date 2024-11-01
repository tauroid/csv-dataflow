from typing import TypeVar
from ..sop import SumProductNode

T = TypeVar("T")


def combine_same(a: T, b: T) -> T:
    assert a == b
    return a


def compute_visible_sop(
    selected: SumProductNode[T, bool],
    expanded: SumProductNode[T, bool],
    parent_expanded: bool = False,
) -> SumProductNode[T] | None:
    node = SumProductNode[T](
        combine_same(selected.sop, expanded.sop),
        {
            path: child
            for path in map(combine_same, selected.children, expanded.children)
            for child in (
                compute_visible_sop(
                    selected.children[path], expanded.children[path], expanded.data
                ),
            )
            if child is not None
        },
    )

    num_children = len(node.children)

    if num_children > 1:
        return node
    elif num_children == 1:
        path, child = next(iter(node.children.items()))

        if node.sop == child.sop:
            return SumProductNode(
                node.sop,
                {
                    f"{path} / {grandchild_path}": grandchild
                    for grandchild_path, grandchild in child.children.items()
                },
            )
        else:
            return node
    elif parent_expanded or selected.data or expanded.data:  # No children but expanded?
        return node
    else:
        return None
