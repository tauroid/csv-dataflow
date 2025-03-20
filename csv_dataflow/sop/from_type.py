from dataclasses import fields, is_dataclass
import types
from typing import Any, TypeVar, get_args, get_origin

from frozendict import frozendict
from csv_dataflow.sop import UNIT, SumOrProduct, SumProductChild, SumProductNode

T = TypeVar("T")


def sop_from_type(t: type[T]) -> SumProductNode[T]:
    sop: SumOrProduct
    child_types: dict[str, Any]
    if get_origin(t) is types.UnionType:
        sop = "+"
        child_types = {
            child_type.__name__: child_type
            for child_type in get_args(t)
        }
    elif is_dataclass(t):
        sop = "*"
        child_types = {
            field.name: field.type for field in fields(t)
        }
    elif get_origin(t) in (tuple, list):
        return SumProductNode(
            "+",
            frozendict[str, SumProductChild](
                {
                    "empty": UNIT,
                    "list": SumProductNode(
                        "*",
                        frozendict[str, SumProductChild](
                            {
                                "head": sop_from_type(
                                    get_args(t)[0]
                                ),
                                "tail": 1,
                            }
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
            {
                key: sop_from_type(child_type)
                for key, child_type in child_types.items()
            }
        ),
    )
