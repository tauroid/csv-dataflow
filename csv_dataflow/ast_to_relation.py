from __future__ import annotations
import ast
from dataclasses import dataclass
from typing import TypeVar

# NOTE this is going to be horrible and wrong for a while
# in the sense of not supporting many parts of Python at all

# NEXT generic ast equality checker, error reporting can be bad

S = TypeVar("S")
T = TypeVar("T")


@dataclass(frozen=True)
class SumType:
    names: tuple[str, ...]

    def __str__(self) -> str:
        return " | ".join(self.names)


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


# def is_of_type(a: SumType, b: SumType) -> bool:
#     if a == b:
#         return True

#     if isinstance(a, type):
#         a = a.__name__
#     elif not isinstance(a, str):
#         raise Exception("dunno")

#     if not isinstance(b, str):
#         if isinstance(b, type) and a == b.__name__:
#             return True
#         elif get_origin(b) == Union and a in {arg.__name__ for arg in get_args(b)}:
#             return True


def parse_as(ast_type: type[T], s: str) -> T:
    module = ast.parse(s)
    assert isinstance(module.body[0], ast_type)
    return module.body[0]


@dataclass(frozen=True)
class MatchBranch:
    match_type_name: str
    return_type_name: str


@dataclass(frozen=True)
class Match:
    match_var_name: str  # Just for rendering, don't get carried away
    match_var_type: SumType
    result_var_name: str  # Same
    result_type: SumType

    branches: tuple[MatchBranch, ...]

    # TODO bring this back at some point
    # def __post_init__(self):
    #     branch_types: list[str] = []
    #     for branch in self.branches:
    #         branch_types.append(branch.match_type_name)
    #         assert is_of_type(branch.match_type_name, self.match_var_type)
    #         assert is_of_type(branch.return_type_name, self.result_type)

    #     # No dupes
    #     assert list(set(branch_types)) == branch_types
    #     # Total
    #     assert set(branch_types) == set(self.match_var_type.names)

    def to_ast(self) -> tuple[ast.AnnAssign, ast.Match]:
        return (
            parse_as(
                ast.AnnAssign,
                f"{self.result_var_name}: {self.result_type}",
            ),
            parse_as(
                ast.Match,
                "\n".join(
                    (
                        f"match {self.match_var_name}:",
                        *(
                            (
                                f"    case {branch.match_type_name}():"
                                f"        {self.result_var_name} = {branch.return_type_name}()"
                            )
                            for branch in self.branches
                        ),
                    )
                ),
            ),
        )

    @staticmethod
    def from_ast(a: tuple[ast.AnnAssign, ast.Match], match_var_type: SumType) -> Match:
        aa, m = a

        branches: list[MatchBranch] = []
        for c in m.cases:
            assert isinstance(c.pattern, ast.MatchClass)
            assert isinstance(c.pattern.cls, ast.Name)
            assign = c.body[0]
            assert isinstance(assign, ast.Assign)
            assert isinstance(assign.value, ast.Call)
            assert isinstance(assign.value.func, ast.Name)
            branches.append(MatchBranch(c.pattern.cls.id, assign.value.func.id))

        assert isinstance(m.subject, ast.Name)
        assert isinstance(aa.target, ast.Name)
        return Match(
            m.subject.id,
            match_var_type,
            aa.target.id,
            annotation_to_sum_type(aa.annotation),
            tuple(branches),
        )


@dataclass(frozen=True)
class Function:
    fn_name: str
    arg_name: str
    arg_type: SumType
    return_type: SumType

    body: Match  # Obviously more to add

    def to_ast(self) -> ast.FunctionDef:
        f = parse_as(
            ast.FunctionDef,
            "\n".join(
                (
                    f"def {self.fn_name}({self.arg_name}: {self.arg_type}) -> {self.return_type}:",
                    "    pass",
                )
            ),
        )
        f.body = [
            *self.body.to_ast(),
            parse_as(ast.Return, f"return {self.body.result_var_name}"),
        ]
        return f

    @staticmethod
    def from_ast(f: ast.FunctionDef) -> Function:
        arg = f.args.args[0]
        assert arg.annotation
        assert f.returns
        arg_type = annotation_to_sum_type(arg.annotation)
        aa, m, _ = f.body
        assert isinstance(aa, ast.AnnAssign)
        assert isinstance(m, ast.Match)
        return Function(
            f.name,
            arg.arg,
            arg_type,
            annotation_to_sum_type(f.returns),
            Match.from_ast((aa, m), arg_type),
        )
