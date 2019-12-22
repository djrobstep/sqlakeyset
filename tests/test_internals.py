from pytest import raises, warns, mark
from sqlalchemy import asc, desc, Column, Integer, String, column
from sqlalchemy.sql.expression import nullslast

from sqlakeyset import OC, Paging, Page
from sqlakeyset import serialize_bookmark
from sqlakeyset.columns import DerivedKey


@mark.filterwarnings("ignore:.*NULLS FIRST.*")
def test_oc():
    a = asc('a')
    b = desc('a')
    c = asc('b')
    n = nullslast(desc('a'))

    a = OC(a)
    b = OC(b)
    c = OC(c)
    n = OC(n)

    assert str(a) == str(OC('a'))
    assert a.is_ascending
    assert not b.is_ascending
    assert not n.reversed.reversed.is_ascending
    assert n.reversed.is_ascending
    assert not n.is_ascending # make sure reversed doesn't modify in-place
    assert str(a.element) == str(b.element) == str(n.element)
    assert str(a) == str(b.reversed)
    assert str(n.reversed.reversed) == str(n)

    assert a.name == 'a'
    assert n.name == 'a'
    assert n.quoted_full_name == 'a'
    assert repr(n) == '<OC: a DESC NULLS LAST>'


def test_okeys():
    a = DerivedKey(OC(asc('a')), lambda x:x)
    b = DerivedKey(OC(desc('b')), lambda x:x)
    assert a.oc.is_ascending
    assert not b.oc.is_ascending


def general_asserts(p):
    if not p.backwards:
        assert p.further == p.next
    else:
        assert p.further == p.previous

    if not p.has_further:
        assert p.current_opposite == (None, not p.backwards)

    assert p.is_full == (len(p.rows) >= p.per_page)
    assert p.bookmark_further == serialize_bookmark(p.further)


def getitem(row, order_cols):
    return tuple(row[c.name] for c in order_cols)


T1 = []
T2 = [
    {'id': 1, 'b': 2},
    {'id': 2, 'b': 1},
    {'id': 3, 'b': 3}
]
T3 = [
    {'id': 1, 'name': 'test'},
    {'id': 2, 'name': 'test1'},
    {'id': 3, 'name': 'test2'},
    {'id': 4, 'name': 'test3'},

]


def test_paging_objects1():
    p = Page(['abc'])
    assert p.one() == 'abc'

    with raises(RuntimeError):
        Page([1, 2]).one()

    with raises(RuntimeError):
        Page([]).one()

    ob = [OC(x) for x in ['id', 'b']]

    p = Paging(T1, 10, ob, backwards=False, current_marker=None, get_marker=getitem)
    assert p.next == (None, False)
    assert p.further == (None, False)
    assert p.previous == (None, True)
    assert not p.is_full
    general_asserts(p)


def test_paging_object2_per_page_3():
    ob = [OC(x) for x in ['id', 'b']]

    p = Paging(T2, 3, ob, backwards=False, current_marker=None, get_marker=getitem)
    assert p.next == ((3, 3), False)
    assert not p.has_next
    assert not p.has_previous
    assert p.further == ((3, 3), False)
    assert p.previous == ((1, 2), True)
    assert p.is_full
    general_asserts(p)


def test_paging_object2_per_page_2():
    ob = [OC(x) for x in ['id', 'b']]

    p = Paging(T2, 2, ob, backwards=False, current_marker=None, get_marker=getitem)
    assert p.next == ((2, 1), False)
    assert p.has_next
    general_asserts(p)

    assert not p.has_previous
    assert p.previous == ((1, 2), True)
    assert p.further == ((2, 1), False)

    general_asserts(p)


def test_paging_object_text():
    ob = [OC(Column('id', Integer, nullable=False)),
          OC(Column('name', String, nullable=False))]

    p = Paging(T3, 2, ob, backwards=False, current_marker=None, get_marker=getitem)

    assert p.rows

    assert p.next == ((2, 'test1'), False)
    assert p.has_next
    general_asserts(p)

    assert not p.has_previous
    assert p.previous == ((1, 'test'), True)
    assert p.further == ((2, 'test1'), False)

    general_asserts(p)

    assert p.further

def test_warn_on_nullslast():
    with warns(UserWarning):
        ob = [OC(nullslast(column('id')))]
        p = Paging(T1, 10, ob, backwards=False, current_marker=None, get_marker=getitem)
