import ast
from dataclasses import dataclass
from frozendict import frozendict
from typing import Any, TypeVar, Union, get_args, get_origin

from .relation import Relation

# NOTE this is going to be horrible and wrong for a while
# in the sense of not supporting many parts of Python at all

S = TypeVar("S")
T = TypeVar("T")


@dataclass(frozen=True)
class SumType:
    names: tuple[str, ...]


T


def annotation_to_sum_type(annotation: ast.expr) -> SumType:
    if isinstance(annotation, ast.Name):
        return SumType((str(annotation.id),))
    elif isinstance(annotation, ast.BinOp):
        assert isinstance(annotation.op, ast.BitOr)
        return SumType(
            (
                *annotation_to_sum_type(annotation.left).names,
                *annotation_to_sum_type(annotation.right).names,
            )
        )
    else:
        raise Exception(f"Type annotation {annotation} is unsupported")


def is_of_type(a: SumType, b: SumType) -> bool:
    if a == b:
        return True

    if isinstance(a, type):
        a = a.__name__
    elif not isinstance(a, str):
        raise Exception("dunno")

    if not isinstance(b, str):
        if isinstance(b, type) and a == b.__name__:
            return True
        elif get_origin(b) == Union and a in {arg.__name__ for arg in get_args(b)}:
            return True


is_of_type("asdkjh", int | str)

# NOTE generally seems like keeping track of free and bound variables will be fun
# Should probably come up with a good way of doing that
# Probably via some IR that's basically ast-lite


@dataclass(frozen=True)
class MatchBranch:
    match_type_name: str
    return_type_name: str


@dataclass(frozen=True)
class Match:
    match_var: str  # Just for rendering, don't get carried away
    result_var: str  # Same
    result_type: SumType

    branches: tuple[MatchBranch, ...]

    def to_ast(self) -> tuple[ast.stmt, ...]:
        return tuple(
            # TODO figure out AnnAssign here
            ast.Match(
            ast.Name(id=self.match_var),
            cases=[
                ast.match_case(
                    pattern=ast.MatchClass(cls=ast.Name(id=branch.match_type_name)),
                    body=[
                        ast.Assign(
                            targets=[ast.Name(id=self.result_var)],
                            value=ast.Call(func=ast.Name(id=branch.return_type_name)),
                        )
                    ],
                )
                for branch in self.branches
            ],
        ))


def ast_to_relation(s: type[S], t: type[T], f: ast.FunctionDef) -> Relation[S, T]:
    assert len(f.args.args) == 1
    arg_name = f.args.args[0].arg
    assert f.args.args[0].annotation
    arg_types = annotation_to_sum_type_names(f.args.args[0].annotation)
    assert f.returns
    return_types = annotation_to_sum_type_names(f.returns)
    assert len(f.body) == 1
    assert isinstance(f.body[0], ast.Match)
    match_stmt = f.body[0]
    assert isinstance(match_stmt.subject, ast.Name)
    assert match_stmt.subject.id == arg_name
    for match_case in match_stmt.cases:
        assert isinstance(match_case.pattern, ast.MatchClass)
        # TODO make sure no declarations in match
        assert isinstance(match_case.pattern.cls, ast.Name)
        assert match_case.pattern.cls.id in arg_types
        assert len(match_case.body) == 1
        assert isinstance(match_case.body[0], ast.Return)
        return_val = match_case.body[0]
        assert isinstance(return_val.value, ast.Call)
        assert isinstance(return_val.value.func, ast.Name)
        assert return_val.value.func.id in return_types
