import ast
import inspect
from typing import Any, Iterator, TypeVar


T = TypeVar("T")

def get_attributes(clsdef: ast.ClassDef) -> Iterator[str]:
    for item in clsdef.body:
        if isinstance(item, ast.AnnAssign):
            assert isinstance(item.target, ast.Name)
            yield item.target.id


def pickler(cls: type[T]) -> type[T]:
    # Adds _session, _init_vars, _cached and _pickler
    # (last one is for identification)
    cls._session = None
    cls._init_vars = {}
    cls._cached = {}
    cls._pickler = True

    clsdef = next(iter(ast.parse(inspect.getsource(cls)).body))
    assert isinstance(clsdef, ast.ClassDef)
    attribute_names = get_attributes(clsdef)

    for name in attribute_names:
        if hasattr(cls, name) and isinstance(attribute := getattr(cls, name), FieldPicker)

    # Deletes fields assigned to FieldPicklers and replaces
    # them with properties using the above

    # Deletes other fields and replaces them with private vars
    # and properties for immutability

    # Adds .attach_session(), which enables use of all the
    # FieldPickler properties by populating _cached and deleting
    # _init_vars. This after recursing through children
    # to also attach_session to any other _picklers

class FieldPickler: ...

def field_pickler(key: str) -> Any:
    return FieldPickler
