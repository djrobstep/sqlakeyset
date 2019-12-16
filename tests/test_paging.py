import warnings

import pytest
from sqlalchemy import select, String, Column, Integer, ForeignKey, column, table, desc, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, aliased
from sqlbag import temporary_database, S

from sqlakeyset import get_page, select_page, serialize_bookmark, unserialize_bookmark, OC, process_args

warnings.simplefilter("error")

Base = declarative_base()

ECHO = False

BOOK = 't_Book'


class Book(Base):
    __tablename__ = BOOK
    id = Column('book_id', Integer, primary_key=True)
    name = Column(String(255))
    a = Column(Integer)
    b = Column(Integer)
    c = Column(Integer)
    d = Column(Integer)
    author_id = Column(Integer, ForeignKey('author.id'))
    prequel_id = Column(Integer, ForeignKey(id), nullable=True)
    prequel = relationship('Book', remote_side=[id],
                           backref='sequel', uselist=False)


class Author(Base):
    __tablename__ = 'author'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    books = relationship('Book', backref='author')


def _dburl(request):
    count = 10
    data = []

    for x in range(count):
        b = Book(name='Book {}'.format(x), a=x, b=x % 2, c=count - x, d=99)

        if x == 1:
            b.a = None
            b.author = Author(name='Willy Shakespeare')

        data.append(b)

    for x in range(count):
        author = Author(name='Author {}'.format(x))
        abooks = []
        for y in range((2*x) % 10):
            b = Book(name='Book {}-{}'.format(x, y), a=x+y, b=(y*x) % 2, c=count - x, d=99-y)
            b.author = author
            if y % 4 != 0:
                b.prequel = abooks[(2*y+1) % len(abooks)]
            abooks.append(b)
            data.append(b)

    with temporary_database(request.param) as dburl:
        with S(dburl) as s:
            Base.metadata.create_all(s.connection())
            s.add_all(data)
        yield dburl

dburl = pytest.fixture(params=['postgresql', 'mysql'])(_dburl)
pg_only_dburl = pytest.fixture(params=['postgresql'])(_dburl)

def check_paging_orm(q):
    item_counts = range(1, 12)

    unpaged = q.all()

    for backwards in [False, True]:
        for per_page in item_counts:
            gathered = []

            page = None, backwards

            while True:
                serialized_page = serialize_bookmark(page)
                page = unserialize_bookmark(serialized_page)

                page_with_paging = get_page(q, per_page=per_page, page=serialized_page)
                paging = page_with_paging.paging

                assert paging.current == page

                if backwards:
                    gathered = page_with_paging + gathered
                else:
                    gathered = gathered + page_with_paging

                page = paging.further

                if not page_with_paging:
                    assert not paging.has_further
                    assert paging.further == paging.current
                    assert paging.current_opposite == (None, not paging.backwards)
                    break

            assert gathered == unpaged


def check_paging_core(selectable, s):
    item_counts = range(1, 12)

    result = s.execute(selectable)
    unpaged = result.fetchall()

    for backwards in [False, True]:
        for per_page in item_counts:
            gathered = []

            page = None, backwards

            while True:
                serialized_page = serialize_bookmark(page)
                page = unserialize_bookmark(serialized_page)

                page_with_paging = select_page(s, selectable, per_page=per_page, page=serialized_page)
                paging = page_with_paging.paging

                assert paging.current == page
                assert page_with_paging.keys() == result.keys()

                if backwards:
                    gathered = page_with_paging + gathered
                else:
                    gathered = gathered + page_with_paging

                page = paging.further

                if not page_with_paging:
                    assert not paging.has_further
                    assert paging.further == paging.current
                    assert paging.current_opposite == (None, not paging.backwards)
                    break

            assert gathered == unpaged


def test_orm_query1(dburl):
    spec = [desc(Book.b), Book.d, Book.id]

    with S(dburl, echo=ECHO) as s:
        q = s.query(Book, Author, Book.id).outerjoin(Author).order_by(*spec)
        check_paging_orm(q=q)


def test_orm_query2(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Book).order_by(Book.id, Book.name)
        check_paging_orm(q=q)


def test_orm_query3(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Book.id, Book.name.label('x')).order_by(Book.name, Book.id)
        check_paging_orm(q=q)


def test_orm_query4(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Book).order_by(Book.name)
        check_paging_orm(q=q)


def test_orm_query_aggregated(dburl):
    count = func.count().label('count')
    spec = [desc(count), desc(Author.name), Author.id]

    with S(dburl, echo=ECHO) as s:
        q = s.query(Author, count).join(Author.books) \
            .group_by(Author).order_by(*spec)
        check_paging_orm(q=q)


def test_orm_query_with_entities(dburl):
    spec = [Author.name, Book.name, desc(Book.id)]

    with S(dburl, echo=ECHO) as s:
        q = s.query(Book).join(Book.author) \
            .filter(Author.name.contains('1')
                    | Author.name.contains('2')) \
            .with_entities(Book.name, Author.name, Book.id) \
            .order_by(*spec)
        check_paging_orm(q=q)


def test_orm_query_subquery(dburl):
    count = func.count().label('count')

    with S(dburl, echo=ECHO) as s:
        sq = s.query(Author.id, count).join(Author.books) \
            .group_by(Author.id).subquery('sq')
        q = s.query(Author).join(sq, sq.c.id == Author.id) \
            .with_entities(sq.c.count, Author) \
            .order_by(desc(sq.c.count), Author.name, Author.id)
        check_paging_orm(q=q)


def test_orm_query_recursive_cte(pg_only_dburl):
    with S(pg_only_dburl, echo=ECHO) as s:
        seed = s.query(Book.id.label('id'), Book.id.label('origin')) \
            .filter(Book.prequel == None)

        # Recurse by picking up sequels
        sequel = aliased(Book, name='sequel')
        recursive = seed.cte(recursive=True)
        recursive = recursive.union(
            s.query(sequel.id, recursive.c.origin)
            .filter(sequel.prequel_id == recursive.c.id)
        )

        # Aggregate and join
        count = func.count().label('count')
        origin = recursive.c.origin.label('origin')
        sq = s.query(origin, count).group_by(origin).cte(recursive=False)

        q = s.query(sq.c.count, Book) \
            .filter(Book.id == sq.c.origin) \
            .order_by(sq.c.count.desc(), Book.id)

        check_paging_orm(q=q)


def test_core(dburl):
    spec = ['b', 'd', 'book_id', 'c']

    cols = [column(each) for each in spec]
    ob = [OC(x).uo for x in spec]

    selectable = select(
        cols,
        from_obj=[table('t_Book')],
        whereclause=column('d') == 99,
        order_by=ob)

    with S(dburl, echo=ECHO) as s:
        check_paging_core(selectable=selectable, s=s)


def test_args():
    assert process_args(page=((1, 2), True)) == ((1, 2), True)
    assert process_args(after=(1, 2)) == ((1, 2), False)
    assert process_args(before=(1, 2)) == ((1, 2), True)

    with pytest.raises(ValueError):
        process_args(before=(1, 2), after=(1, 2))
    with pytest.raises(ValueError):
        process_args(before=(1, 2), page=(1, 2))
    with pytest.raises(ValueError):
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
