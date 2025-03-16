from typing import Any, TypeVar, TypeIs

T = TypeVar("T")


def assert_true(value: bool) -> bool:
    assert value
    return True


def assert_isinstance(value: Any, typ: type[T], yes: bool = True) -> TypeIs[T]:
    """
    Make `yes` false if you want to assert it's not an instance
    then return False
    """
    assert isinstance(value, typ) == yes
    return yes
