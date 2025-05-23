import ast
from dataclasses import dataclass, fields, is_dataclass
import inspect
import pickle
from typing import (
    Any,
    Generic,
    Iterator,
    MutableMapping,
    Protocol,
    TypeVar,
    cast,
)

T = TypeVar("T")


def get_attributes(clsdef: ast.ClassDef) -> Iterator[str]:
    for item in clsdef.body:
        if isinstance(item, ast.AnnAssign):
            assert isinstance(item.target, ast.Name)
            yield item.target.id


class NoAttachedKVStoreError(Exception):
    def __init__(self):
        super().__init__(
            "No key value store to read from (call"
            " .attach_pickle_store on this pickler or a parent)."
            " Possibly if your picklers are nested there's an"
            " intermediate class that is not a pickler, and so"
            " doesn't pass through the KV store"
        )


@dataclass(frozen=True)
class _FieldPickler(Generic[T]):
    class Unset: ...

    init_value: T | Unset = Unset()


def field_pickler(*args: T) -> T:
    return cast(T, _FieldPickler(*args))  # liiieeessss


@dataclass
class _KVStoreFieldReference:
    name: str
    """
    Index in _init_vars
    (as that's defined before we know what the prefix is)
    """

    def key(self, prefix: str | None) -> str:
        """Index in every other pickler impl attribute"""
        if prefix is None:
            raise NoAttachedKVStoreError()

        return f"{prefix}_{self.name}"


class _FieldPicklerGet(Generic[T], _KVStoreFieldReference):
    def __call__(self, parent: Any) -> T:
        if parent._store is None:
            return parent._init_vars[self.name]

        key = self.key(parent._prefix)

        if key in parent._cached:
            return parent._cached[key]

        if key in parent._store:
            value = pickle.loads(parent._store[key])
        else:
            assert self.name in parent._init_vars
            value = parent._init_vars[self.name]
            parent._store[key] = pickle.dumps(value)

        parent._cached[key] = value

        return value


class _FieldPicklerSet(Generic[T], _KVStoreFieldReference):
    def __call__(self, parent: Any, value: T) -> None:
        key = self.key(parent._prefix)
        parent._store[key] = pickle.dumps(value)
        parent._cached[key] = value


@dataclass
class _OrdinaryField(Generic[T]):
    name: str

    def __call__(self, parent: Any) -> T:
        return parent._init_vars[self.name]


class _AttachPickleStoreProtocol(Protocol):
    def __call__(
        self,
        store: MutableMapping[str, bytes],
        prefix: str,
        parent: Any | None,
    ) -> None: ...


@dataclass(frozen=True)
class _AttachPickleStore(Generic[T]):
    field_picklers: tuple[str, ...]
    ordinary_fields: tuple[str, ...]

    def __get__(
        self, obj: Any, objtype: type[Any]
    ) -> _AttachPickleStoreProtocol:
        def f(
            store: MutableMapping[str, bytes],
            prefix: str,
            parent: Any | None = None,
        ) -> None:
            obj._prefix = prefix
            obj._parent = parent
            obj._store = store

            for name in self.field_picklers:
                # Bring initvar through into the store
                getattr(obj, name)

            for name in self.ordinary_fields:
                ordinary_field = getattr(obj, name)
                if hasattr(
                    ordinary_field, "_attach_pickle_store"
                ):
                    ordinary_field._attach_pickle_store(
                        store, f"{prefix}_{name}", obj
                    )

        return f


def _pickler_constructor(
    cls: type[Any], ordinary_fields: tuple[str, ...]
) -> Any:
    def c(obj: Any, *args: Any, **kwargs: Any) -> Any:
        obj._prefix = None
        obj._init_vars = cls._init_vars.copy()
        obj._cached = cls._cached.copy()

        for kw, arg in kwargs.items():
            obj._init_vars[kw] = arg

        for arg, field in zip(args, fields(cls)):
            assert field.name not in obj._init_vars
            obj._init_vars[field.name] = arg

        missing_args: list[str] = []
        for name in ordinary_fields:
            if name not in obj._init_vars:
                missing_args.append(name)

        if missing_args:
            raise TypeError(
                f"{cls.__name__} constructor missing args"
                f" {missing_args}"
            )

    return c


class NotADataclass(Exception):
    def __init__(self):
        super().__init__("A @pickler must also be a @dataclass")


def pickler(cls_t: type[T]) -> type[T]:
    """Only works on non-frozen dataclasses"""
    if not is_dataclass(cls_t):
        raise NotADataclass()

    cls = cast(type[Any], cls_t)

    cls._parent = None
    cls._prefix = None
    cls._store = None
    init_vars: dict[str, Any] = {}
    cls._init_vars = init_vars
    cls._cached = {}

    (clsdef,) = ast.parse(
        inspect.cleandoc(inspect.getsource(cls))
    ).body
    assert isinstance(clsdef, ast.ClassDef)
    attribute_names = get_attributes(clsdef)

    field_picklers: list[str] = []
    ordinary_fields: list[str] = []
    for name in attribute_names:
        # If there's no default value, it's not a pickler
        if not hasattr(cls, name):
            ordinary_fields.append(name)
            setattr(
                cls, name, property(_OrdinaryField[Any](name))
            )
            continue

        attribute = getattr(cls, name)
        if not isinstance(attribute, _FieldPickler):
            ordinary_fields.append(name)
            # Put the (class) default value in _init_vars
            cls._init_vars[name] = attribute
            # Make the field read only
            setattr(
                cls, name, property(_OrdinaryField[Any](name))
            )
        else:
            field_picklers.append(name)

            attribute = cast(_FieldPickler[Any], attribute)

            # Field picklers can have default values too
            if not isinstance(
                attribute.init_value, _FieldPickler.Unset
            ):
                cls._init_vars[name] = attribute.init_value

            # Make reads and writes go through a property
            setattr(
                cls,
                name,
                property(
                    fget=_FieldPicklerGet[Any](name),
                    fset=_FieldPicklerSet[Any](name),
                ),
            )

    cls._attach_pickle_store = _AttachPickleStore(
        tuple(field_picklers), tuple(ordinary_fields)
    )

    cls.__init__ = _pickler_constructor(
        cls, tuple(ordinary_fields)
    )

    return cls


def attach_pickle_store(
    obj: Any, store: MutableMapping[str, bytes], prefix: str
):
    obj._attach_pickle_store(store, prefix)
