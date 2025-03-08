from __future__ import annotations
import ast
from dataclasses import dataclass
from functools import cache
from typing import Any

from csv_dataflow.ast_to_relation import parse_as
from csv_dataflow.ast_to_relation.types import SumType, annotation_to_sum_type
from csv_dataflow.relation import BasicRelation, Between, ParallelRelation, Relation
from csv_dataflow.sop import UNIT, SumProductNode

from .match import Match

@dataclass(frozen=True)
class Function:
    fn_name: str
    arg_name: str
    arg_type: SumType
    return_type: SumType

    body: Match  # Obviously more to add

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

    @property
    @cache
    def as_ast(self) -> ast.FunctionDef:
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

    @property
    @cache
    def as_sops_and_relation(
        self,
    ) -> tuple[SumProductNode[Any], SumProductNode[Any], Relation[Any, Any]]:
        return (
            self.arg_type.as_sop,
            self.return_type.as_sop,
            ParallelRelation(
                tuple(
                    (
                        BasicRelation(UNIT, UNIT),
                        Between((branch.match_type_name,), (branch.return_type_name,)),
                    )
                    for branch in self.body.branches
                )
            ),
        )
