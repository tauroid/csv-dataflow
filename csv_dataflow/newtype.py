from dataclasses import dataclass
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class NewType(Generic[T]):
    value: T

    def __str__(self) -> str:
        return str(self.value)

    def __eq__(self, other: Any) -> bool:
        return other == self.value
