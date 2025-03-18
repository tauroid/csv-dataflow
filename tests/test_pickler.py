import pickle
import pytest

from dataclasses import dataclass
from csv_dataflow.gui.session.pickler import (
    NoAttachedKVStoreError,
    NotADataclass,
    attach_pickle_store,
    field_pickler,
    pickler,
)


def test_blank_pickler():
    @pickler
    @dataclass
    class Thing: ...

    Thing()


def test_ordinary_field():
    @pickler
    @dataclass
    class Thing:
        x: int

    thing = Thing(5)
    assert 5 == thing.x
    with pytest.raises(AttributeError):
        thing.x = 6


def test_non_dataclass_pickler():
    with pytest.raises(NotADataclass):

        @pickler
        class _: ...


def test_pickler_init():
    @pickler
    @dataclass
    class Thing:
        x: int = field_pickler()

    thing = Thing(6)
    store: dict[str, bytes] = {}
    attach_pickle_store(thing, store, "thing")
    assert 6 == pickle.loads(store["thing_x"])


def test_pickler_set():
    @pickler
    @dataclass
    class Thing:
        x: int = field_pickler()

    thing = Thing(6)
    store: dict[str, bytes] = {}
    attach_pickle_store(thing, store, "thing")
    thing.x = 7
    assert 7 == pickle.loads(store["thing_x"])


def test_pickler_set_deep():
    @pickler
    @dataclass
    class Thing3:
        x: int = field_pickler()

    @pickler
    @dataclass
    class Thing2:
        thing3: Thing3

    @pickler
    @dataclass
    class Thing1:
        thing2: Thing2

    thing1 = Thing1(Thing2(Thing3(5)))

    store: dict[str, bytes] = {}
    attach_pickle_store(thing1, store, "thing1")
    assert 5 == pickle.loads(store["thing1_thing2_thing3_x"])
    thing1.thing2.thing3.x = 10
    assert 10 == pickle.loads(store["thing1_thing2_thing3_x"])


def test_pickler_missing_args():
    @pickler
    @dataclass
    class Thing:
        x: int
        y: str = field_pickler()

    with pytest.raises(TypeError):
        Thing(5)


def test_pickler_default_args():
    @pickler
    @dataclass
    class Thing:
        x: int
        y: str = field_pickler("boo")
        z: bool = False

    thing = Thing(5)
    attach_pickle_store(thing, {}, "thing")
    assert "boo" == thing.y
    assert False == thing.z


def test_pickler_no_store():
    @pickler
    @dataclass
    class Thing:
        x: int
        y: str = field_pickler("boo")
        z: bool = False

    thing = Thing(5)
    assert False == thing.z
    with pytest.raises(NoAttachedKVStoreError):
        thing.y
