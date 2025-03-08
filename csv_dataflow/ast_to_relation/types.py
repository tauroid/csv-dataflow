import ast
from dataclasses import dataclass
from functools import cache
from typing import Any

from csv_dataflow.sop import UNIT, SumProductNode


@dataclass(frozen=True)
class SumType:
    names: tuple[str, ...]

    def __str__(self) -> str:
        return " | ".join(self.names)

    @property
    @cache
    def as_sop(self) -> SumProductNode[Any]:
        return SumProductNode("+", {name: UNIT for name in self.names})


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
