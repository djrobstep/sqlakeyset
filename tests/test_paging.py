import warnings

from sqlbag import temporary_database, S
from sqlalchemy import select, String, Column, Integer, ForeignKey, column, table, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from sqlakeyset import get_page, select_page, serialize_bookmark, unserialize_bookmark, OC, process_args

from pytest import raises

warnings.simplefilter("error")

Base = declarative_base()

ECHO = False

BOOK = 't_Book'


class Book(Base):
    __tablename__ = BOOK
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    a = Column(Integer)
    b = Column(Integer)
    c = Column(Integer)
    d = Column(Integer)
    author_id = Column(Integer, ForeignKey('author.id'))


class Author(Base):
    __tablename__ = 'author'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    books = relationship('Book', backref='author')


def fixture_setup(dburl):
    COUNT = 10

    with S(dburl) as s:
        Base.metadata.create_all(s.connection())

    with S(dburl) as s:
        for x in range(COUNT):
            b = Book(
                name='Book {}'.format(x),
                a=x,
                b=x % 2,
                c=COUNT - x,
                d=99)

            if x == 1:
                b.a = None
                b.author = Author(name='Willy Shakespeare')
            s.add(b)


def check_paging(q=None, selectable=None, s=None):
    ITEM_COUNTS = range(1, 12)

    if q is not None:
        unpaged = q.all()
    elif selectable is not None:
        result = s.execute(selectable)

        unpaged = result.fetchall()

    for per_page in ITEM_COUNTS:
        for backwards in [False, True]:

            gathered = []

            page = None, backwards

            while True:
                serialized_page = serialize_bookmark(page)
                page = unserialize_bookmark(serialized_page)

                if q is not None:
                    method = get_page
                    args = (q,)
                elif selectable is not None:
                    method = select_page
                    args = (s, selectable)

                rows = method(
                    *args,
                    per_page=per_page,
                    page=serialized_page
                )

                p = rows.paging

                assert p.current == page

                if selectable is not None:
                    assert rows.keys() == result.keys()

                if backwards:
                    gathered = rows + gathered
                else:
                    gathered = gathered + rows

                page = p.further

                if not rows:
                    assert not p.has_further
                    assert p.further == p.current
                    assert p.current_opposite == (None, not p.backwards)
                    break

            assert gathered == unpaged


def do_orm_tests(dburl):
    spec = [desc(Book.b), Book.d, Book.id]

    with S(dburl, echo=ECHO) as s:
        q = s.query(Book, Author, Book.id).outerjoin(Author).order_by(*spec)
        q2 = s.query(Book).order_by(Book.id, Book.name)
        q3 = s.query(Book.id, Book.name.label('x')).order_by(Book.name, Book.id)
        q4 = s.query(Book).order_by(Book.name)
        q5 = s.query(Book).order_by(Book.id)

        check_paging(q=q)
        check_paging(q=q2)
        check_paging(q=q3)
        check_paging(q=q4)
        check_paging(q=q5)


def do_core_tests(dburl):
    spec = ['b', 'd', 'id', 'c']

    cols = [column(each) for each in spec]
    ob = [OC(x).uo for x in spec]

    with S(dburl, echo=ECHO) as s:
        selectable = select(
            cols,
            from_obj=[table('t_Book')],
            whereclause=column('d') == 99,
            order_by=ob)

        check_paging(selectable=selectable, s=s)


def test_args():
    assert process_args(page=((1, 2), True)) == ((1, 2), True)
    assert process_args(after=(1, 2)) == ((1, 2), False)
    assert process_args(before=(1, 2)) == ((1, 2), True)

    with raises(ValueError):
        process_args(before=(1, 2), after=(1, 2))
    with raises(ValueError):
        process_args(before=(1, 2), page=(1, 2))
    with raises(ValueError):
        process_args(after=(1, 2), page=(1, 2))
    assert process_args(False, False, False) == (None, False)


def test_bookmarks():
    def twoway(x):
        before = x
        ss = serialize_bookmark(x)
        after = unserialize_bookmark(ss)
        return before == after

    first = (None, False)
    last = (None, True)

    assert serialize_bookmark(first) == '>'
    assert serialize_bookmark(last) == '<'
    assert twoway(first)
    assert twoway(last)


def test_paging():
    for db in ['postgresql', 'mysql']:
        with temporary_database(db) as dburl:
            fixture_setup(dburl)
            do_orm_tests(dburl)
            do_core_tests(dburl)
