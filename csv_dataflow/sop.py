from dataclasses import dataclass, fields, is_dataclass, replace
from pprint import pprint
import types
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    TypeVar,
    cast,
    get_args,
    get_origin,
)

from frozendict import frozendict

SumOrProduct = Literal["+", "*"]


T = TypeVar("T")
Data = TypeVar("Data", default=None)


type SumProductPath[T] = tuple[str, ...]


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
    children: Mapping[str, "SumProductNode[Any, Data]"]
    """The key is the path member"""
    data: Data = cast(Data, None)

    def at(self, path: SumProductPath[T]) -> "SumProductNode[Any,Data]":
        if not path:
            return self

        return self.children[path[0]].at(path[1:])

    def replace_at(
        self, path: SumProductPath[T], node: "SumProductNode[Any,Data]"
    ) -> "SumProductNode[T, Data]":
        assert path
        if len(path) == 1:
            return replace(self, children={**self.children, path[0]: node})
        else:
            return replace(
                self,
                children=frozendict(
                    {
                        **self.children,
                        path[0]: self.children[path[0]].replace_at(path[1:], node),
                    }
                ),
            )

    def replace_data_at(
        self, path: SumProductPath[T], data: Data
    ) -> "SumProductNode[T, Data]":
        return self.replace_at(path, replace(self.at(path), data=data))

    def clip_path(
        self, path: SumProductPath[T], prefix: SumProductPath[T] = ()
    ) -> SumProductPath[T]:
        if not self.children:
            return prefix
        else:
            assert path
            return self.children[path[0]].clip_path(path[1:], (*prefix, path[0]))


UNIT = SumProductNode[Any]("*", frozendict[str, SumProductNode[Any]]({}))


def sop_from_type(t: type[T]) -> SumProductNode[T]:
    sop: SumOrProduct
    child_types: dict[str, Any]
    if get_origin(t) is types.UnionType:
        sop = "+"
        child_types = {child_type.__name__: child_type for child_type in get_args(t)}
    elif is_dataclass(t):
        sop = "*"
        child_types = {field.name: field.type for field in fields(t)}
    else:
        # Assume remaining types are primitive i.e. sum
        # Not actually true, will want to think about lists etc
        sop = "+"
        child_types = {}

    return SumProductNode(
        sop,
        frozendict[str, SumProductNode[Any]](
            {key: sop_from_type(child_type) for key, child_type in child_types.items()}
        ),
    )


def iter_sop_paths(
    node: SumProductNode[T, Data], prefix: SumProductPath[T] = ()
) -> Iterator[SumProductPath[T]]:
    if not node.children:
        yield prefix
    else:
        for child_path, child in node.children.items():
            for path in iter_sop_paths(child, (*prefix, child_path)):
                yield path


def add_paths(
    node: SumProductNode[T], paths: Iterable[SumProductPath[T]]
) -> SumProductNode[T]:
    paths_tuple = tuple(paths)

    def paths_at(element: str) -> Iterator[SumProductPath[T]]:
        for path in paths_tuple:
            if path[0] == element and len(path) > 1:
                yield path[1:]

    if node.children:
        children = frozendict[str, SumProductNode[T]](
            {
                child_path: add_paths(node.children[child_path], paths_at(child_path))
                for child_path in node.children
            }
        )
    else:
        children = frozendict[str, SumProductNode[T]](
            {
                child_path: add_paths(
                    SumProductNode("+", {}),
                    paths_at(child_path),
                )
                for child_path in {path[0]: None for path in paths_tuple}.keys()
            }
        )

    return SumProductNode(
        node.sop,
        children,
        node.data,
    )


def select_from_paths(
    node: SumProductNode[T, Data], paths: Collection[SumProductPath[T]]
) -> SumProductNode[T, Data]:
    return SumProductNode(
        node.sop,
        frozendict[str, SumProductNode[T, Data]](
            {
                child_path: select_from_paths(
                    node.children[child_path],
                    {
                        path[1:]
                        for path in paths
                        if path[0] == child_path and len(path) > 1
                    },
                )
                for child_path in {path[0] for path in paths}
            }
        ),
        node.data,
    )


A = TypeVar("A")
B = TypeVar("B")


def map_node_data(
    f: Callable[[A], B], node: SumProductNode[T, A]
) -> SumProductNode[T, B]:
    return cast(
        SumProductNode[T, B],
        replace(
            node,
            data=f(node.data),
            children=frozendict(
                {path: map_node_data(f, child) for path, child in node.children.items()}
            ),
        ),
    )


def clip_sop(
    node: SumProductNode[T, Data], clip: SumProductNode[T, Any]
) -> SumProductNode[T, Data]:
    return SumProductNode(
        node.sop,
        frozendict[str, SumProductNode[T, Data]](
            {
                path: clip_sop(node_child, clip_child)
                for path, clip_child in clip.children.items()
                for node_child in (node.children.get(path),)
                if node_child
            }
        ),
        node.data,
    )
