from dataclasses import dataclass, fields, is_dataclass, replace
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

from .cons import Cons, ConsList, at_index

SumOrProduct = Literal["+", "*"]

DeBruijn = int


T = TypeVar("T")
Data = TypeVar("Data", default=None)


type SumProductPath[T] = tuple[str, ...]

type SumProductChild[Data=None] = SumProductNode[Any, Data] | DeBruijn


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

    def at(
        self,
        path: SumProductPath[T],
        prev_stack: "ConsList[SumProductNode[Any,Data]]" = None,
    ) -> "SumProductNode[Any,Data]":
        if not path:
            return self

        stack = Cons(self, prev_stack)

        child = self.children[path[0]]
        path_tail = path[1:]

        if isinstance(child, int):
            return at_index(stack, child).at(path_tail, stack)
        else:
            return child.at(path_tail, stack)

    def replace_at(
        self,
        path: SumProductPath[T],
        node: "SumProductNode[Any,Data]",
        prev_stack: "ConsList[SumProductNode[Any,Data]]" = None,
    ) -> "SumProductNode[T, Data]":
        assert path
        if len(path) == 1:
            return replace(self, children={**self.children, path[0]: node})
        else:
            stack = Cons(self, prev_stack)

            child = self.children[path[0]]
            if isinstance(child, int):
                unrolled_child = at_index(stack, child)
            else:
                unrolled_child = child

            return replace(
                self,
                children=frozendict(
                    {
                        **self.children,
                        path[0]: unrolled_child.replace_at(path[1:], node, stack),
                    }
                ),
            )

    def replace_data_at(
        self, path: SumProductPath[T], data: Data
    ) -> "SumProductNode[T, Data]":
        return self.replace_at(path, replace(self.at(path), data=data))


UNIT = SumProductNode[Any]("*", frozendict[str, SumProductChild]({}))
VOID = SumProductNode[Any]("+", frozendict[str, SumProductChild]({}))


def sop_from_type(t: type[T]) -> SumProductNode[T]:
    sop: SumOrProduct
    child_types: dict[str, Any]
    if get_origin(t) is types.UnionType:
        sop = "+"
        child_types = {child_type.__name__: child_type for child_type in get_args(t)}
    elif is_dataclass(t):
        sop = "*"
        child_types = {field.name: field.type for field in fields(t)}
    # elif get_origin(t) in (tuple, list):
    #     return SumProductNode(
    #         "+",
    #         frozendict[str, SumProductChild](
    #             {
    #                 "empty": UNIT,
    #                 "list": SumProductNode(
    #                     "*",
    #                     frozendict[str, SumProductChild](
    #                         {"head": sop_from_type(get_args(t)[0]), "tail": 1}
    #                     ),
    #                 ),
    #             }
    #         ),
    #     )
    else:
        # Assume remaining types are primitive i.e. sum
        sop = "+"
        child_types = {}

    return SumProductNode(
        sop,
        frozendict[str, SumProductChild](
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
            assert not isinstance(
                child, int
            ), "Flat iterating over a recursive type is probably a mistake"
            for path in iter_sop_paths(child, (*prefix, child_path)):
                yield path


def add_paths(
    node: SumProductNode[T],
    paths: Iterable[SumProductPath[T]],
    prev_stack: ConsList[SumProductNode[Any]] = None,
) -> SumProductNode[T]:
    paths_tuple = tuple(paths)

    def paths_at(element: str) -> Iterator[SumProductPath[T]]:
        for path in paths_tuple:
            if path[0] == element and len(path) > 1:
                yield path[1:]

    if node.children:
        stack = Cons(node, prev_stack)
        children_dict: dict[str, SumProductChild] = {}
        for child_path, child in node.children.items():
            if isinstance(child, int):
                unrolled_child = at_index(stack, child)
            else:
                unrolled_child = child

            children_dict[child_path] = add_paths(
                unrolled_child, paths_at(child_path), stack
            )

        children = frozendict[str, SumProductChild](children_dict)
    else:
        children = frozendict[str, SumProductChild](
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
    node: SumProductNode[T, Data],
    paths: Collection[SumProductPath[T]],
    prev_stack: ConsList[SumProductNode[Any, Data]] = None,
) -> SumProductNode[T, Data]:
    stack = Cons(node, prev_stack)

    children_dict: dict[str, SumProductChild[Data]] = {}
    for child_path in {path[0]: None for path in paths}.keys():
        child = node.children[child_path]
        if isinstance(child, int):
            unrolled_child = at_index(stack, child)
        else:
            unrolled_child = child

        children_dict[child_path] = select_from_paths(
            unrolled_child,
            {
                path[1:]: None
                for path in paths
                if path[0] == child_path and len(path) > 1
            }.keys(),
            stack,
        )

    return SumProductNode(
        node.sop,
        frozendict[str, SumProductNode[T, Data]](children_dict),
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
            children=frozendict[str, SumProductChild[B]](
                {
                    path: (
                        map_node_data(f, child) if not isinstance(child, int) else child
                    )
                    for path, child in node.children.items()
                }
            ),
        ),
    )


def clip_sop(
    node: SumProductNode[T, Data],
    clip: SumProductNode[T, Any],
    prev_stack: ConsList[SumProductNode[Any, Data]] = None,
) -> SumProductNode[T, Data]:
    stack = Cons(node, prev_stack)

    children_dict: dict[str, SumProductChild[Data]] = {}
    for path, clip_child in clip.children.items():
        assert not isinstance(
            clip_child, int
        ), "Potentially this could make sense but we haven't needed it yet"

        node_child = node.children.get(path)
        if isinstance(node_child, int):
            unrolled_node_child = at_index(stack, node_child)
        else:
            unrolled_node_child = node_child

        if unrolled_node_child:
            children_dict[path] = clip_sop(unrolled_node_child, clip_child)

    return SumProductNode(
        node.sop,
        frozendict[str, SumProductChild[Data]](children_dict),
        node.data,
    )
