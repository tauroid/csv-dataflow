from dataclasses import replace
from itertools import chain
from typing import TypeVar, cast

from frozendict import frozendict
from csv_dataflow.sop import SumProductChild, SumProductNode

T = TypeVar("T")
Data = TypeVar("Data", default=None)


def merge(
    *sops: SumProductNode[T, Data]
) -> SumProductNode[T, Data]:
    first, *rest = sops

    assert all(
        sop.sop == first.sop and sop.data == first.data
        for sop in rest
    )

    child_paths = tuple(
        map(lambda sop: sop.children.keys(), sops)
    )

    all_child_paths = {
        x: None for x in chain.from_iterable(child_paths)
    }.keys()

    recursion_paths: list[str] = []

    # Make sure they have the same recursion structure, not dealing
    # with it otherwise
    for path in all_child_paths:
        first_child, *rest_children = filter(
            None, (sop.children.get(path) for sop in sops)
        )
        if isinstance(first_child, int):
            for child in rest_children:
                assert first_child == child
            recursion_paths.append(path)
        else:
            for child in rest_children:
                # This should make the cast below safe
                assert not isinstance(child, int)

    return replace(
        first,
        children=frozendict[str, SumProductChild[Data]](
            {
                path: (
                    children[0]
                    if path in recursion_paths
                    else merge(
                        *cast(
                            tuple[SumProductNode[T, Data], ...],
                            children,
                        )
                    )
                )
                for path in all_child_paths
                for children in (
                    tuple(
                        filter(
                            None,
                            (
                                sop.children.get(path)
                                for sop in sops
                            ),
                        )
                    ),
                )
            }
        ),
    )
