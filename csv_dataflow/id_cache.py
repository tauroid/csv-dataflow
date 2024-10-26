from functools import wraps
from typing import Any, Callable, TypeVar

S = TypeVar("S")
T = TypeVar("T")

_id_cache: dict[int, Any] = {}

def id_cache(f: Callable[[S], T]) -> Callable[[S], T]:
    @wraps(f)
    def wrapped(x: S) -> T:
        id_x = id(x)

        if id_x not in _id_cache:
            _id_cache[id_x] = f(x)

        return _id_cache[id_x]

    return wrapped
