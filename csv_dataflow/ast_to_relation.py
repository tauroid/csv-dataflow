import ast
from typing import TypeVar

from .relation import Relation

# NOTE this is going to be horrible and wrong for a while

S = TypeVar("S")
T = TypeVar("T")

def annotation_to_sum_type_names(annotation: ast.expr) -> tuple[str, ...]:
    if isinstance(annotation, ast.Name):
        return str(annotation.id),
    elif isinstance(annotation, ast.BinOp):
        assert isinstance(annotation.op, ast.BitOr)
        return (
            *annotation_to_sum_type_names(annotation.left),
            *annotation_to_sum_type_names(annotation.right)
        )
    else:
        raise Exception(f"Type annotation {annotation} is unsupported")

# NOTE generally seems like keeping track of free and bound variables will be fun
# Should probably come up with a good way of doing that
# Probably via some IR that's basically ast-lite

def ast_to_relation(s: type[S], t: type[T], f: ast.FunctionDef) -> Relation[S,T]:
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
