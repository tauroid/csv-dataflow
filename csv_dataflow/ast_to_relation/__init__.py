from __future__ import annotations
import ast
from typing import TypeVar


# NOTE this is going to be horrible and wrong for a while
# in the sense of not supporting many parts of Python at all

# NEXT Function (as it currently stands) to Relation, then add to home screen
# Probably some typechecking along the way
# I suppose that would happen by first constructing the types (SOPs)
# and then not letting the relation escape them

# So no need to do it until conversion to relation though at some
# point nice error reporting has to come in (at the point of
# conversion I suppose, though that seems to mean text range
# information has to percolate deeply into the process)
#
# Remember the IR is just a parse-dont-validate stepping stone and
# can be whatever I want once function-to-relation is set up and working

S = TypeVar("S")
T = TypeVar("T")



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


def asts_equal(a: ast.AST, b: ast.AST) -> bool:
    # When can this be wrong?
    return ast.dump(a) == ast.dump(b)
