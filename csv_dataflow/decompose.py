import ast
from dataclasses import dataclass
import inspect
from types import FunctionType
from typing import Callable, Generic, ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class DecomposedFunction(Generic[P,R]):
    """
    Just for keeping both around for now but the real thing will
    be just storing the original function somewhere in Relation
    """
    original: Callable[P,R]
    ast: ast.FunctionDef

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.original(*args, **kwargs)

def decompose(f: Callable[P,R]) -> DecomposedFunction[P,R]:
    # This cast may well be wrong but putting FunctionType as a
    # bound on F means the use site complains (for some reason)
    m = ast.parse(inspect.getsource(cast(FunctionType, f)))
    assert len(m.body) == 1
    f_def = m.body[0]
    # Anyway we'll fail here if it wasn't a function so no big
    assert isinstance(f_def, ast.FunctionDef)
    return DecomposedFunction(f, f_def)
