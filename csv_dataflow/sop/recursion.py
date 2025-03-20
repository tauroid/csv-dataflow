from typing import TypeVar
from csv_dataflow.sop import SumProductNode

T = TypeVar("T")
Data = TypeVar("Data", default=None)


def only_has_de_bruijn_indices(sop: SumProductNode[T, Data]) -> bool:
    if not sop.children:
        # Terminal node is data
        return False

    return all(
        isinstance(child, int)
        or only_has_de_bruijn_indices(child)
        for child in sop.children.values()
    )


def max_de_bruijn_index_relative_to_current_node(
    sop: SumProductNode[T, Data],
) -> int:
    """0 is the argument, 1 is node above, etc"""

    if not sop.children:
        # This suits if the purpose is just to tell if the node doesn't
        # refer outside itself
        return 0

    return max(
        (
            child
            if isinstance(child, int)
            else max_de_bruijn_index_relative_to_current_node(
                child
            )
            - 1
        )
        for child in sop.children.values()
    )


def is_empty_recursion(sop: SumProductNode[T, Data]) -> bool:
    return (
        only_has_de_bruijn_indices(sop)
        and max_de_bruijn_index_relative_to_current_node(sop)
        <= 0
    )
