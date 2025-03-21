from __future__ import annotations
from dataclasses import dataclass, replace
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Iterator,
    Literal,
    Mapping,
    TypeVar,
    cast,
)

from frozendict import frozendict


from ..cons import ConsList

SumOrProduct = Literal["+", "*"]

DeBruijn = int


T = TypeVar("T")
Data = TypeVar("Data", default=None)
OtherData = TypeVar("OtherData")


type SumProductPath[T] = tuple[str, ...]

type SumProductChild[Data = None] = SumProductNode[
    Any, Data
] | DeBruijn


@dataclass(frozen=True)
class SumProductNode(Generic[T, Data]):
    """
    Because of Python being Python, children of a Sum will all
    be Products (or neither Sum nor Product), but children of
    a Product can be either (or neither)

    (because Unions are not tagged)

    We'll just pretend that's a possibility anyway
    """

    sop: SumOrProduct
    children: Mapping[str, SumProductChild[Data]]
    """The key is the path member"""
    data: Data = cast(Data, None)

    @staticmethod
    def from_type(t: type[T]) -> SumProductNode[T]:
        return sop_from_type(t)

    def at(
        self,
        path: SumProductPath[T],
        prev_stack: ConsList[SumProductNode[Any, Data]] = None,
    ) -> SumProductNode[Any, Data]:
        return at(self, path, prev_stack)

    def replace_at(
        self,
        path: SumProductPath[T],
        node: SumProductNode[Any, Data],
        prev_stack: ConsList[SumProductNode[Any, Data]] = None,
    ) -> SumProductNode[T, Data]:
        return replace_at(self, path, node, prev_stack)

    def replace_data_at(
        self, path: SumProductPath[T], data: Data
    ) -> SumProductNode[T, Data]:
        return replace_data_at(self, path, data)

    def clip(
        self,
        clip_sop: SumProductNode[T, Any],
        prev_stack: ConsList[SumProductNode[Any, Data]] = None,
    ) -> SumProductNode[T, Data]:
        return clip(self, clip_sop, prev_stack)

    def clip_path(
        self,
        path: SumProductPath[T],
        path_prefix: SumProductPath[T] = (),
        prev_stack: ConsList[SumProductNode[Any, Data]] = None,
    ) -> SumProductPath[T]:
        return clip_path(self, path, path_prefix, prev_stack)

    def iter_leaf_paths(
        self, prefix: SumProductPath[T] = ()
    ) -> Iterator[SumProductPath[T]]:
        return iterate_leaves(self, prefix)

    def iter_all_paths(
        self, prefix: SumProductPath[T] = ()
    ) -> Iterator[SumProductPath[T]]:
        return iterate_every(self, prefix)

    def add_values_at_paths(
        self, paths: Collection[SumProductPath[T]]
    ) -> SumProductNode[T, Data]:
        added = add_values_at_paths(self, paths)
        assert not isinstance(added, DeBruijn)
        return added

    def filter_to_paths(
        self, paths: Collection[SumProductPath[T]]
    ) -> SumProductNode[T, Data] | None:
        return filter_to_paths(self, paths)

    def map_data(
        self, f: Callable[[Data], OtherData]
    ) -> SumProductNode[T, OtherData]:
        return cast(
            SumProductNode[T, OtherData],
            replace(
                self,
                data=f(self.data),
                children=frozendict[
                    str, SumProductChild[OtherData]
                ](
                    {
                        path: (
                            child.map_data(f)
                            if not isinstance(child, int)
                            else child
                        )
                        for path, child in self.children.items()
                    }
                ),
            ),
        )

    def merge(
        self, *sops: SumProductNode[T, Data]
    ) -> SumProductNode[T, Data]:
        return merge(self, *sops)

    def only_has_de_bruijn_indices(self) -> bool:
        return only_has_de_bruijn_indices(self)

    def max_de_bruijn_index_relative_to_current_node(
        self,
    ) -> int:
        return max_de_bruijn_index_relative_to_current_node(self)

    def is_empty_recursion(self) -> bool:
        """Has no content, just infinite loops"""
        return is_empty_recursion(self)


UNIT = SumProductNode[Any](
    "*", frozendict[str, SumProductChild]({})
)
VOID = SumProductNode[Any](
    "+", frozendict[str, SumProductChild]({})
)

# Implementations
from csv_dataflow.sop.at import at, replace_at, replace_data_at
from csv_dataflow.sop.clip import clip, clip_path
from csv_dataflow.sop.from_type import sop_from_type
from csv_dataflow.sop.merge import merge
from csv_dataflow.sop.paths.add_values import add_values_at_paths
from csv_dataflow.sop.paths.iterate import (
    iterate_every,
    iterate_leaves,
)
from csv_dataflow.sop.paths.filter_to import filter_to_paths
from csv_dataflow.sop.recursion import (
    is_empty_recursion,
    max_de_bruijn_index_relative_to_current_node,
    only_has_de_bruijn_indices,
)
