from __future__ import annotations
import ast
from dataclasses import dataclass

from csv_dataflow.ast_to_relation import parse_as
from csv_dataflow.ast_to_relation.types import SumType, annotation_to_sum_type


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
