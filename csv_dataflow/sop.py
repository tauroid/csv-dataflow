from dataclasses import dataclass, fields, is_dataclass, replace
import types
from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    Mapping,
    MutableMapping,
    TypeVar,
    cast,
    get_args,
    get_origin,
)

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


UNIT = SumProductNode[Any]("*", {})


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
        sop, {key: sop_from_type(child_type) for key, child_type in child_types.items()}
    )


def add_value_to_path(sop: SumProductNode[T], path: SumProductPath[T], value: str):
    if len(path) == 0:
        children = cast(MutableMapping[str, SumProductNode[Any]], sop.children)
        assert value not in children
        children[value] = UNIT
    else:
        add_value_to_path(sop.children[path[0]], path[1:], value)


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
            children={
                path: map_node_data(f, child) for path, child in node.children.items()
            },
        ),
    )
