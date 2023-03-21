from packaging import version
from pytest import mark, raises, warns
from sqlalchemy import Column, Integer, String, asc, column, desc
from sqlalchemy.orm import class_mapper

from sqlakeyset.sqla import SQLA_VERSION

if SQLA_VERSION >= version.parse("1.4"):
    from sqlalchemy.orm import declarative_base
else:
    from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import nullslast
from sqlalchemy.sql.operators import asc_op  # , desc_op

from sqlakeyset import Page, Paging, serialize_bookmark
from sqlakeyset.columns import (
    OC,
    derive_order_key,
    AppendedColumn,
    DirectColumn,
    AttributeColumn,
    _get_order_direction,
    _remove_order_direction,
    _reverse_order_direction,
)


@mark.filterwarnings("ignore:.*NULLS FIRST.*")
def test_oc():
    a = asc("a")
    b = desc("a")
    c = asc("b")
    n = nullslast(desc("a"))

    a = OC(a)
    b = OC(b)
    c = OC(c)
    n = OC(n)

    assert str(a) == str(OC("a"))
    assert a.is_ascending
    assert not b.is_ascending
    assert not n.reversed.reversed.is_ascending
    assert n.reversed.is_ascending
    assert not n.is_ascending  # make sure reversed doesn't modify in-place
    assert str(a.element) == str(b.element) == str(n.element)
    assert str(a) == str(b.reversed)
    assert str(n.reversed.reversed) == str(n)

    assert a.name == "a"
    assert n.name == "a"
    assert n.quoted_full_name == "a"
    assert repr(n) == "<OC: a DESC NULLS LAST>"


def test_order_manipulation():
    def is_asc(c):
        return _get_order_direction(c) == asc_op

    flip = _reverse_order_direction
    scrub = _remove_order_direction
    base = column("a")
    base_label = base.label("test")
    a = asc(base)
    d = desc(base)
    assert is_asc(a)
    assert not is_asc(d)
    equal_pairs = [
        (scrub(a), base),
        (scrub(d), base),
        (scrub(asc(base_label)), scrub(a.label("test"))),
        (flip(a), d),
        (flip(d), a),
    ]
    for lhs, rhs in equal_pairs:
        assert str(lhs) == str(rhs)


def test_mappedocols():
    a = AppendedColumn(OC(asc("a")))
    b = DirectColumn(OC(desc("b")), 0)
    assert a.oc.is_ascending
    assert not b.oc.is_ascending
    assert b.reversed.oc.is_ascending
    assert b.reversed.oc.is_ascending


def test_flask_sqla_compat():
    # test djrobstep#18 for regression
    class T(declarative_base()):
        __tablename__ = "t"
        i = Column(Integer, primary_key=True)

    desc = {
        "name": "T",
        "type": T,
        "aliased": False,
        "expr": class_mapper(T),
        "entity": T,
    }
    mapping = derive_order_key(OC(T.i), desc, 0)
    assert isinstance(mapping, AttributeColumn)


def general_asserts(p):
    if not p.backwards:
        assert p.further == p.next
    else:
        assert p.further == p.previous

    if not p.has_further:
        assert p.current_opposite == (None, not p.backwards)

    assert p.is_full == (len(p.rows) >= p.per_page)
    assert p.bookmark_further == serialize_bookmark(p.further)


def keys_of(rows, order_cols):
    return [tuple(row[c.name] for c in order_cols) for row in rows]


T1 = []
T2 = [{"id": 1, "b": 2}, {"id": 2, "b": 1}, {"id": 3, "b": 3}]
T3 = [
    {"id": 1, "name": "test"},
    {"id": 2, "name": "test1"},
    {"id": 3, "name": "test2"},
    {"id": 4, "name": "test3"},
]


def test_paging_objects1():
    p = Page(["abc"], None)  # type: ignore
    assert p.one() == "abc"

    with raises(RuntimeError):
        Page([1, 2], None).one()  # type: ignore

    with raises(RuntimeError):
        Page([], None).one()  # type: ignore

    ob = [OC(x) for x in ["id", "b"]]

    p = Paging(T1, 10, backwards=False, current_place=None, places=keys_of(T1, ob))
    assert p.next == (None, False)
    assert p.further == (None, False)
    assert p.previous == (None, True)
    assert not p.is_full
    general_asserts(p)


def test_paging_object2_per_page_3():
    ob = [OC(x) for x in ["id", "b"]]

    p = Paging(T2, 3, backwards=False, current_place=None, places=keys_of(T2, ob))
    assert p.next == ((3, 3), False)
    assert not p.has_next
    assert not p.has_previous
    assert p.further == ((3, 3), False)
    assert p.previous == ((1, 2), True)
    assert p.is_full
    general_asserts(p)


def test_paging_object2_per_page_2():
    ob = [OC(x) for x in ["id", "b"]]

    p = Paging(T2, 2, backwards=False, current_place=None, places=keys_of(T2, ob))
    assert p.next == ((2, 1), False)
    assert p.has_next
    general_asserts(p)

    assert not p.has_previous
    assert p.previous == ((1, 2), True)
    assert p.further == ((2, 1), False)

    general_asserts(p)


def test_paging_object_text():
    ob = [
        OC(Column("id", Integer, nullable=False)),
        OC(Column("name", String, nullable=False)),
    ]

    p = Paging(T3, 2, backwards=False, current_place=None, places=keys_of(T3, ob))

    assert p.rows

    assert p.next == ((2, "test1"), False)
    assert p.has_next
    general_asserts(p)

    assert not p.has_previous
    assert p.previous == ((1, "test"), True)
    assert p.further == ((2, "test1"), False)


def test_paging_backwards_from_none():
    ob = [
        OC(Column("id", Integer, nullable=False)),
        OC(Column("name", String, nullable=False)),
    ]
    T3r = list(reversed(T3))

    p = Paging(T3r, 2, backwards=True, current_place=None, places=keys_of(T3r, ob))

    assert p.rows

    assert p.next == ((4, "test3"), False)
    assert not p.has_next
    general_asserts(p)

    assert p.has_previous
    assert p.has_further
    assert p.previous == ((3, "test2"), True)
    assert p.further == ((3, "test2"), True)


def test_paging_backwards_from_place():
    ob = [
        OC(Column("id", Integer, nullable=False)),
        OC(Column("name", String, nullable=False)),
    ]
    T3r3 = list(reversed(T3[:-1]))

    p = Paging(
        T3r3,
        2,
        backwards=True,
        current_place=(4, "test3"),
        places=keys_of(T3r3, ob),
    )

    assert p.rows

    assert p.has_next
    assert p.next == ((3, "test2"), False)
    general_asserts(p)

    assert p.has_previous
    assert p.has_further
    assert p.previous == ((2, "test1"), True)
    assert p.further == ((2, "test1"), True)


def test_warn_on_nullslast():
    with warns(UserWarning):
        ob = [OC(nullslast(column("id")))]
        Paging(T1, 10, backwards=False, current_place=None, places=keys_of(T1, ob))
