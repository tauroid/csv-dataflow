from dataclasses import dataclass, fields, is_dataclass, replace
from itertools import chain
import types
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
    elif get_origin(t) in (tuple, list):
        return SumProductNode(
            "+",
            frozendict[str, SumProductChild](
                {
                    "empty": UNIT,
                    "list": SumProductNode(
                        "*",
                        frozendict[str, SumProductChild](
                            {"head": sop_from_type(get_args(t)[0]), "tail": 1}
                        ),
                    ),
                }
            ),
        )
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


def add_path_values(
    node: SumProductNode[T] | DeBruijn,
    paths: Collection[SumProductPath[T]],
    prev_stack: ConsList[SumProductNode[Any]] = None,
    active_paths: Collection[SumProductPath[T]] = (),
    recursing: bool = False,
) -> SumProductNode[T] | DeBruijn:
    """
    Find the penultimate node of each path and add the final
    value to it as a child

    The paths don't have to be anchored at the root, any sequence
    of branches that matches works
    """
    if isinstance(node, int):
        if recursing:
            return node

        node = at_index(prev_stack, node)
        recursing = True

    paths = (*paths, *active_paths)

    values = tuple(path[0] for path in paths if len(path) == 1)

    children = frozendict[str, SumProductChild](
        {
            **{value: UNIT for value in values},
            **{
                child_path: add_path_values(
                    child,
                    paths,
                    Cons(node, prev_stack),
                    tuple(path[1:] for path in paths if path[0] == child_path),
                    recursing,
                )
                for child_path, child in node.children.items()
            },
        }
    )

    return SumProductNode(
        node.sop,
        children,
        node.data,
    )


def select_from_paths(
    node: SumProductNode[T, Data] | DeBruijn,
    paths: Collection[SumProductPath[T]],
    prev_stack: ConsList[SumProductNode[Any, Data]] = None,
) -> SumProductNode[T, Data] | None:
    """These paths have to be anchored at the root"""
    if not paths:
        return None

    if isinstance(node, int):
        node = at_index(prev_stack, node)

    if () in paths:
        return node

    children = frozendict[str, SumProductChild[Data]](
        {
            child_path: filtered_child
            for child_path, child in node.children.items()
            for filtered_child in (
                select_from_paths(
                    child,
                    tuple(path[1:] for path in paths if path[0] == child_path),
                    Cons(node, prev_stack),
                ),
            )
            if filtered_child is not None
        }
    )

    if not children:
        return None

    return SumProductNode(
        node.sop,
        children,
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


def merge_sops(*sops: SumProductNode[T, Data]) -> SumProductNode[T, Data]:
    first, *rest = sops

    assert all(sop.sop == first.sop and sop.data == first.data for sop in rest)

    child_paths = tuple(map(lambda sop: sop.children.keys(), sops))

    all_child_paths = {x: None for x in chain.from_iterable(child_paths)}.keys()

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
                    else merge_sops(
                        *cast(tuple[SumProductNode[T, Data], ...], children)
                    )
                )
                for path in all_child_paths
                for children in (
                    tuple(filter(None, (sop.children.get(path) for sop in sops))),
                )
            }
        ),
    )


def only_has_de_bruijn_indices(sop: SumProductNode[T]) -> bool:
    if not sop.children:
        # Terminal node is data
        return False

    return all(
        isinstance(child, int) or only_has_de_bruijn_indices(child)
        for child in sop.children.values()
    )


def max_de_bruijn_index_relative_to_current_node(sop: SumProductNode[T]) -> int:
    """0 is the argument, 1 is node above, etc"""

    if not sop.children:
        # This suits if the purpose is just to tell if the node doesn't
        # refer outside itself
        return 0

    return max(
        (
            child
            if isinstance(child, int)
            else max_de_bruijn_index_relative_to_current_node(child) - 1
        )
        for child in sop.children.values()
    )


def empty_recursion(sop: SumProductNode[T]) -> bool:
    return (
        only_has_de_bruijn_indices(sop)
        and max_de_bruijn_index_relative_to_current_node(sop) <= 0
    )
