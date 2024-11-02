from typing import Generic, Iterable, Mapping, TypeVar

from csv_dataflow.decompose import decompose
from csv_dataflow.newtype import NewType

T = TypeVar("T")


class Single(Generic[T], NewType[T]): ...


class Multiple(Generic[T], NewType[Iterable[T]]): ...


@decompose
def input_translation(input: str) -> Single[int] | Multiple[int]:
    if input.startswith("multi"):
        return Multiple([1, 2, 3, 4])
    else:
        return Single(sum(map(ord, input)))


@decompose
def filterpool(
    inputs: Iterable[tuple[str, int]], output_mapping: Mapping[int, str]
) -> Iterable[tuple[str, int]]:
    translated_inputs = ((input_translation(input), data) for (input, data) in inputs)

    pool: list[tuple[int, int]] = []
    for input, data in translated_inputs:
        if isinstance(input, Single):
            pool.append((input.value, data))
        else:
            pool.extend((element, data) for element in input.value)

    outputs: list[tuple[str, int]] = []
    for source_tag, target_tag in output_mapping.items():
        full_data: int | None = None
        for tag, data in pool:
            if tag == source_tag:
                if full_data is None:
                    full_data = data
                else:
                    full_data += data

        if full_data is not None:
            outputs.append((target_tag, full_data))

    return outputs
