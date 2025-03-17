import ast
from dataclasses import dataclass
import inspect
import pickle
from typing import Any, Generic, Iterator, TypeVar, cast

from flask.sessions import SessionMixin

T = TypeVar("T")


def get_attributes(clsdef: ast.ClassDef) -> Iterator[str]:
    for item in clsdef.body:
        if isinstance(item, ast.AnnAssign):
            assert isinstance(item.target, ast.Name)
            yield item.target.id


class NoSessionError(Exception):
    def __init__(self):
        super().__init__(
            "No session to read from (call .attach_session on"
            " this pickler or a parent)"
        )


class FieldPickler: ...


class FieldPicklerGet(Generic[T]):
    name: str | None

    def __call__(self, parent: Any) -> T:
        if self.name is None:
            raise NoSessionError()

        if self.name in parent._cached:
            return parent._cached[self.name]
        else:
            if self.name in parent._session:
                value = pickle.loads(parent._session[self.name])
            else:
                assert self.name in parent._init_vars
                value = parent._init_vars[self.name]
                parent._session[self.name] = pickle.dumps(value)

            parent._cached[self.name] = value
            return value


class FieldPicklerSet(Generic[T]):
    name: str | None

    def __call__(self, parent: Any, value: T) -> None:
        if self.name is None:
            raise NoSessionError()

        parent._cached[self.name] = value
        parent._session[self.name] = pickle.dumps(value)


class OrdinaryField(Generic[T]):
    name: str

    def __call__(self, parent: Any) -> T:
        return parent._init_vars[self.name]


@dataclass(frozen=True)
class AttachSession(Generic[T]):
    field_picklers: tuple[str, ...]

    def __call__(self, parent: Any, session: SessionMixin, prefix: str) -> None:
        parent._session = session

        for name in self.field_picklers:
            session_key = f"{prefix}_{name}"
            pickler = getattr(parent, name)
            pickler.fget.name = session_key
            pickler.fset.name = session_key


def pickler(cls_t: type[T]) -> type[T]:
    cls = cast(type[Any], cls_t)

    # Adds _session, _init_vars, _cached and _pickler
    # (last one is for identification)
    cls._session = None
    cls._init_vars = {}
    cls._cached = {}
    cls._pickler = True

    (clsdef,) = ast.parse(inspect.getsource(cls)).body
    assert isinstance(clsdef, ast.ClassDef)
    attribute_names = get_attributes(clsdef)

    field_picklers: list[str] = []
    for name in attribute_names:
        attribute = getattr(cls, name)
        # Deletes fields assigned to FieldPicklers and replaces
        # them with properties using the above
        if attribute is FieldPickler:
            field_picklers.append(name)
            setattr(
                cls,
                name,
                property(fget=FieldPicklerGet[Any](), fset=FieldPicklerSet[Any]()),
            )

    # Deletes other fields and replaces them with private vars
    # and properties for immutability

    # Adds .attach_session(), which enables use of all the
    # FieldPickler properties by populating _cached and deleting
    # _init_vars. This after recursing through children
    # to also attach_session to any other _picklers
    cls.attach_session = AttachSession(tuple(field_picklers))


def field_pickler(key: str) -> Any:
    return FieldPickler
