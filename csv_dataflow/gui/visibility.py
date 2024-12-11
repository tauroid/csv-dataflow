from typing import TypeVar

from frozendict import frozendict

from ..sop import DeBruijn, SumOrProduct, SumProductChild, SumProductNode

S = TypeVar("S")
T = TypeVar("T")


def combine_same(a: T, b: T) -> T:
    assert a == b
    return a


def compute_visible_sop(
    selected: SumProductNode[T, bool] | None,
    expanded: SumProductNode[T, bool] | None,
    parent_expanded: bool = True,
) -> SumProductNode[T, bool] | None:
    sop: SumOrProduct
    if selected is None and expanded is None:
        return None
    elif selected and expanded:
        child_paths = map(combine_same, selected.children, expanded.children)
        sop = combine_same(selected.sop, expanded.sop)
    elif selected:
        child_paths = selected.children.keys()
        sop = selected.sop
    elif expanded:
        child_paths = expanded.children.keys()
        sop = expanded.sop
    else:
        # Shouldn't happen
        raise NotImplementedError

    def int_to_none(v: int | SumProductNode[T, bool]) -> SumProductNode[T, bool] | None:
        return None if isinstance(v, int) else v

    visible_children: dict[str, SumProductChild[bool]] = {
        path: child
        for path in child_paths
        for child in (
            compute_visible_sop(
                int_to_none(selected.children[path]) if selected else None,
                int_to_none(expanded.children[path]) if expanded else None,
                expanded.data if expanded else False,
            ),
        )
        if child is not None
    }

    node = SumProductNode[T, bool](
        sop,
        frozendict[str, SumProductChild[bool]](visible_children),
        (selected.data if selected else False)
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
