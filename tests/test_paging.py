import enum
import warnings
from random import randrange
from packaging import version

import pytest
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import (
    select,
    String,
    Column,
    Enum,
    Integer,
    ForeignKey,
    column,
    table,
    desc,
    func,
    inspect,
)

from sqlakeyset.sqla import SQLA_VERSION
if SQLA_VERSION >= version.parse("2.0"):
    from sqlalchemy.orm import declarative_base
else:
    from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, aliased, column_property, Bundle
from sqlalchemy.types import TypeDecorator
from sqlbag import temporary_database, S

import arrow
from datetime import timedelta

# XXX monkeypatch until sqlalchemy_utils supports sqlalchemy 1.4
try:
    from sqlalchemy.orm.query import _ColumnEntity
except ImportError:
    from sqlalchemy.orm.context import _ColumnEntity
    import sqlalchemy.orm.query

    sqlalchemy.orm.query._ColumnEntity = _ColumnEntity

from sqlalchemy_utils import ArrowType

from sqlakeyset import (
    get_page,
    select_page,
    serialize_bookmark,
    unserialize_bookmark,
    custom_bookmark_type,
    InvalidPage,
)
from sqlakeyset.paging import process_args
from sqlakeyset.columns import OC

warnings.simplefilter("error")


class Base(declarative_base()):
    __abstract__ = True

    def __repr__(self):
        try:
            name = type(self).__name__
            cols = inspect(type(self)).columns.keys()
            colstr = ", ".join(f"{k}={getattr(self, k)!r}" for k in cols)
            return f"{name}({colstr})"
        except Exception:  # e.g. if instance is stale and detached
            return super().__repr__()


ECHO = False

BOOK = "t_Book"

custom_bookmark_type(arrow.Arrow, "da", deserializer=arrow.get)


def randtime():
    return arrow.now() - timedelta(seconds=randrange(86400))


# Custom type to guard against double processing: see issue #47
class MyInteger(float):
    pass


custom_bookmark_type(MyInteger, "mi")


class DoubleResultProcessing(Exception):
    pass


class GuardDoubleResultProcessing(TypeDecorator):
    impl = Integer
    cache_ok = True

    def process_result_value(self, value, dialect):
        if isinstance(value, MyInteger):
            raise DoubleResultProcessing(
                "Result processor was called on an already processed value!"
            )
        return MyInteger(value)

    def process_bind_param(self, value, dialect):
        return float(value)


class Colour(enum.Enum):
    red = 0
    green = 1
    blue = 2


custom_bookmark_type(
    Colour,
    "col",
    serializer=lambda c: c.name,
    deserializer=lambda s: Colour[s],
)


class Light(Base):
    __tablename__ = "t_Light"
    id = Column(Integer, primary_key=True)
    intensity = Column(Integer, nullable=False)
    colour = Column(Enum(Colour), nullable=False)
    myint = Column(GuardDoubleResultProcessing, nullable=False)


class Book(Base):
    __tablename__ = BOOK
    id = Column("book_id", Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    a = Column(Integer)
    b = Column(Integer, nullable=False)
    c = Column(Integer, nullable=False)
    d = Column(Integer, nullable=False)
    author_id = Column(Integer, ForeignKey("author.id"))
    prequel_id = Column(Integer, ForeignKey(id), nullable=True)
    prequel = relationship("Book", remote_side=[id], backref="sequel", uselist=False)
    published_at = Column(ArrowType, default=randtime, nullable=False)

    popularity = column_property(b + c * d)

    @hybrid_property
    def score(self):
        return self.b * self.c - self.d


class Author(Base):
    __tablename__ = "author"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    books = relationship("Book", backref="author")
    info = Column(String(255), nullable=False)

    @hybrid_property
    def book_count(self):
        return len(self.books)

    @book_count.expression
    def book_count(cls):
        return (
            select([func.count(Book.id)])
            .where(Book.author_id == cls.id)
            .label("book_count")
        )


JoinedInheritanceBase = declarative_base()


class Animal(JoinedInheritanceBase):
    # These columns all have weird names to test we're not relying on defaults
    id = Column("anim_id", Integer, primary_key=True)
    category = Column("cat", String(255), nullable=False)
    name = Column("nnom", String(255))
    leg_count = Column("lc", Integer, nullable=False, default=0)

    __tablename__ = "inh_animal"
    __mapper_args__ = {
        "polymorphic_on": "category",
        "polymorphic_identity": "animal",
    }


class Invertebrate(Animal):
    id = Column("invid", Integer, ForeignKey(Animal.id), primary_key=True)
    __tablename__ = "inh_invertebrate"
    __mapper_args__ = {
        "polymorphic_identity": "invertebrate",
    }


class Vertebrate(Animal):
    id = Column("random_column_name", Integer, ForeignKey(Animal.id), primary_key=True)
    vertebra_count = Column(Integer, nullable=False, default=0)
    __tablename__ = "inh_vertebrate"
    __mapper_args__ = {
        "polymorphic_identity": "vertebrate",
    }


class Arthropod(Invertebrate):
    id = Column("unrelated", Integer, ForeignKey(Invertebrate.id), primary_key=True)
    __tablename__ = "inh_arthropod"
    __mapper_args__ = {
        "polymorphic_identity": "arthropod",
    }


class Mammal(Vertebrate):
    id = Column("mamamammal", Integer, ForeignKey(Vertebrate.id), primary_key=True)
    nipple_count = Column(Integer, nullable=False, default=0)
    __tablename__ = "inh_mammal"
    __mapper_args__ = {
        "polymorphic_identity": "mammal",
    }


def _dburl(request):
    count = 10
    data = []

    for x in range(count):
        b = Book(name="Book {}".format(x), a=x, b=x % 2, c=count - x, d=99)

        if x == 1:
            b.a = None
            b.author = Author(name="Willy Shakespeare", info="Old timer")

        data.append(b)

    for x in range(count):
        author = Author(
            name="Author {}".format(x), info="Rank {}".format(count + 1 - x)
        )
        abooks = []
        for y in range((2 * x) % 10):
            b = Book(
                name="Book {}-{}".format(x, y),
                a=x + y,
                b=(y * x) % 2,
                c=count - x,
                d=99 - y,
            )
            b.author = author
            if y % 4 != 0:
                b.prequel = abooks[(2 * y + 1) % len(abooks)]
            abooks.append(b)
            data.append(b)

    data += [
        Light(colour=Colour(i % 3), intensity=(i * 13) % 53, myint=i) for i in range(99)
    ]

    with temporary_database(request.param, host="localhost") as dburl:
        with S(dburl) as s:
            Base.metadata.create_all(s.connection())
            s.add_all(data)
        yield dburl


#SUPPORTED_ENGINES = ["sqlite", "postgresql", "mysql"]
SUPPORTED_ENGINES = ["sqlite"]

dburl = pytest.fixture(params=SUPPORTED_ENGINES)(_dburl)
#no_mysql_dburl = pytest.fixture(params=["sqlite", "postgresql"])(_dburl)
no_mysql_dburl = pytest.fixture(params=["sqlite"])(_dburl)
pg_only_dburl = pytest.fixture(params=["postgresql"])(_dburl)


@pytest.fixture(params=SUPPORTED_ENGINES)
def joined_inheritance_dburl(request):
    with temporary_database(request.param, host="localhost") as dburl:
        with S(dburl) as s:
            JoinedInheritanceBase.metadata.create_all(s.connection())
            s.add_all(
                [
                    Mammal(
                        name="Human", vertebra_count=33, leg_count=2, nipple_count=2
                    ),
                    Mammal(name="Dog", vertebra_count=36, leg_count=4, nipple_count=10),
                    Invertebrate(name="Jellyfish"),
                    Invertebrate(name="Jellyfish"),
                    Arthropod(name="Spider", leg_count=8),
                    Arthropod(name="Ant", leg_count=6),
                    Arthropod(name="Scorpion", leg_count=8),
                    Arthropod(name="Beetle", leg_count=6),
                    Vertebrate(name="Snake", vertebra_count=300),
                ]
            )
        yield dburl


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

                if len(gathered) < len(unpaged):
                    # Ensure each page is the correct size
                    assert paging.has_further
                    assert len(page_with_paging) == per_page
                else:
                    assert not paging.has_further

                if not page_with_paging:
                    assert not paging.has_further
                    assert paging.further == paging.current
                    assert paging.current_opposite == (None, not paging.backwards)
                    break

            # Ensure union of pages is original q.all()
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

                page_with_paging = select_page(
                    s, selectable, per_page=per_page, page=serialized_page
                )
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
        q = s.query(Book).only_return_tuples(True).order_by(Book.id, Book.name)
        check_paging_orm(q=q)


def test_orm_query3(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Book.id, Book.name.label("x")).order_by(Book.name, Book.id)
        check_paging_orm(q=q)


def test_orm_query4(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Book).order_by(Book.name)
        check_paging_orm(q=q)


def test_orm_bad_page(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Book).order_by(Book.name)

        # check that malformed page tuple fails
        with pytest.raises(InvalidPage):
            get_page(q, per_page=10, page=((1,), False, "Potatoes"))

        # one order col, so check place with 2 elements fails
        with pytest.raises(InvalidPage):
            get_page(q, per_page=10, page=((1, 1), False))


def test_orm_order_by_arrowtype(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Book).order_by(Book.published_at, Book.id)
        check_paging_orm(q=q)


def test_orm_implicit_join(dburl):
    with S(dburl, echo=ECHO) as s:
        q = (
            s.query(Book)
            .order_by(Author.name, Book.id)
            .filter(Book.author_id == Author.id)
        )
        check_paging_orm(q=q)


def test_orm_hybrid_property(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Book).order_by(Book.score, Book.id)
        check_paging_orm(q=q)
        q = s.query(Book, Author).join(Book.author).order_by(Book.score, Book.id)
        check_paging_orm(q=q)
        q = (
            s.query(Book.score, Book, Author)
            .join(Book.author)
            .order_by(Book.score, Book.id)
        )
        check_paging_orm(q=q)


def test_orm_column_property(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Book).order_by(Book.popularity, Book.id)
        check_paging_orm(q=q)
        q = (
            s.query(Book, Author)
            .join(Book.author)
            .order_by(Book.popularity.desc(), Book.id)
        )
        check_paging_orm(q=q)


def test_column_named_info(dburl):
    # See issue djrobstep#24
    with S(dburl, echo=ECHO) as s:
        q = s.query(Author).from_self().order_by(Author.info, Author.id)
        check_paging_orm(q=q)


def test_orm_correlated_subquery_hybrid(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Author.id, Author.name).order_by(Author.book_count, Author.id)
        check_paging_orm(q=q)


def test_orm_expression(dburl):
    with S(dburl, echo=ECHO) as s:
        key = func.coalesce(Book.a, 0) + Book.b
        q = s.query(Book).order_by(key, Book.id)
        check_paging_orm(q=q)

        q = s.query(Book).order_by(key.label("sort_by_me"), Book.id)
        check_paging_orm(q=q)


def test_orm_aggregated(dburl):
    count = func.count().label("count")
    spec = [desc(count), desc(Author.name), Author.id]

    with S(dburl, echo=ECHO) as s:
        q = s.query(Author, count).join(Author.books).group_by(Author).order_by(*spec)
        check_paging_orm(q=q)


def test_orm_with_entities(dburl):
    spec = [Author.name, Book.name, desc(Book.id)]

    with S(dburl, echo=ECHO) as s:
        q = (
            s.query(Book)
            .join(Book.author)
            .filter(Author.name.contains("1") | Author.name.contains("2"))
            .with_entities(Book.name, Author.name, Book.id)
            .order_by(*spec)
        )
        check_paging_orm(q=q)


def test_orm_subquery(dburl):
    count = func.count().label("count")

    with S(dburl, echo=ECHO) as s:
        sq = (
            s.query(Author.id, count)
            .join(Author.books)
            .group_by(Author.id)
            .subquery("sq")
        )
        q = (
            s.query(Author)
            .join(sq, sq.c.id == Author.id)
            .with_entities(sq.c.count, Author)
            .order_by(desc(sq.c.count), Author.name, Author.id)
        )
        check_paging_orm(q=q)


@pytest.mark.skip
def test_orm_recursive_cte(pg_only_dburl):
    with S(pg_only_dburl, echo=ECHO) as s:
        # Start with "origins": books that don't have prequels
        seed = s.query(Book.id.label("id"), Book.id.label("origin")).filter(
            Book.prequel is None
        )

        # Recurse by picking up sequels
        sequel = aliased(Book, name="sequel")
        recursive = seed.cte(recursive=True)
        recursive = recursive.union(
            s.query(sequel.id, recursive.c.origin).filter(
                sequel.prequel_id == recursive.c.id
            )
        )

        # Count total books per origin
        count = func.count().label("count")
        origin = recursive.c.origin.label("origin")
        sq = s.query(origin, count).group_by(origin).cte(recursive=False)

        # Join to full book table
        q = (
            s.query(sq.c.count, Book)
            .filter(Book.id == sq.c.origin)
            .order_by(sq.c.count.desc(), Book.id)
        )

        check_paging_orm(q=q)


bundle_bug = version.parse(sqlalchemy.__version__) == version.parse("1.4.0b1")


@pytest.mark.skipif(
    bundle_bug, reason="https://github.com/sqlalchemy/sqlalchemy/issues/5702"
)
def test_orm_order_by_bundle(dburl):
    Scorecard = Bundle(
        "scorecard",
        # CW: existential horror
        Book.score.label("popularity"),
        Book.popularity.label("score"),
    )

    with S(dburl, echo=ECHO) as s:
        q = s.query(Book).order_by(Scorecard, Book.id)
        check_paging_orm(q=q)
        q = s.query(Book, Scorecard).order_by(Book.id)
        check_paging_orm(q=q)
        q = s.query(Scorecard).order_by(Scorecard.c.popularity, Book.id)
        check_paging_orm(q=q)


def test_orm_joined_inheritance(joined_inheritance_dburl):
    with S(joined_inheritance_dburl, echo=ECHO) as s:
        q = s.query(Animal).order_by(Animal.leg_count, Animal.id)
        check_paging_orm(q=q)

        q = s.query(Vertebrate).order_by(Vertebrate.vertebra_count, Animal.id)
        check_paging_orm(q=q)

        q = s.query(Mammal).order_by(Mammal.nipple_count, Mammal.leg_count, Mammal.id)
        check_paging_orm(q=q)

        # Mix up accessing columns at various heirarchy levels
        q = s.query(Mammal).order_by(
            Mammal.nipple_count, Mammal.leg_count, Vertebrate.vertebra_count, Animal.id
        )
        check_paging_orm(q=q)


def test_core(dburl):
    spec = ["b", "d", "book_id", "c"]

    cols = [column(each) for each in spec]
    ob = [OC(x).uo for x in spec]

    selectable = select(
        cols, from_obj=[table("t_Book")], whereclause=column("d") == 99, order_by=ob
    )

    with S(dburl, echo=ECHO) as s:
        check_paging_core(selectable=selectable, s=s)

    # Check again with a connection instead of session (see #37):
    with S(dburl, echo=ECHO) as s:
        check_paging_core(selectable=selectable, s=s.connection())


def test_core2(dburl):
    with S(dburl, echo=ECHO) as s:
        sel = select([Book.score]).order_by(Book.id)
        check_paging_core(sel, s)

        sel = (
            select([Book.score])
            .order_by(Author.id - Book.id, Book.id)
            .where(Author.id == Book.author_id)
        )
        check_paging_core(sel, s)

        sel = (
            select([Book.author_id, func.count()])
            .group_by(Book.author_id)
            .order_by(func.sum(Book.popularity))
        )
        check_paging_core(sel, s)

        v = func.sum(func.coalesce(Book.a, 0)) + func.min(Book.b)
        sel = (
            select([Book.author_id, func.count(), v])
            .group_by(Book.author_id)
            .order_by(v)
        )
        check_paging_core(sel, s)


def test_core_enum(dburl):
    with S(dburl, echo=ECHO) as s:
        selectable = select([Light.id, Light.colour]).order_by(
            Light.intensity, Light.id
        )
        check_paging_core(selectable=selectable, s=s)


# MySQL sorts enums by index in ORDER BY clauses, but treats them as
# strings in ROW() constructors, and thus compares them by their labels;
# so we don't test ordering by enums in MySQL.  If users want to do
# this, they need to ensure that their enums are defined in alphabetical
# order (as recommended by the MySQL documentation).
def test_core_order_by_enum(no_mysql_dburl):
    with S(no_mysql_dburl, echo=ECHO) as s:
        selectable = select([Light.id, Light.colour]).order_by(
            Light.colour, Light.intensity, Light.id
        )
        check_paging_core(selectable=selectable, s=s)


def test_core_result_processor(dburl):
    with S(dburl, echo=ECHO) as s:
        selectable = select([Light.id, Light.myint]).order_by(Light.intensity, Light.id)
        check_paging_core(selectable=selectable, s=s)


def test_core_order_by_result_processor(dburl):
    with S(dburl, echo=ECHO) as s:
        selectable = select([Light.id]).order_by(Light.myint, Light.id)
        check_paging_core(selectable=selectable, s=s)


def test_orm_enum(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Light.id, Light.colour).order_by(Light.intensity, Light.id)
        check_paging_orm(q=q)


def test_orm_order_by_enum(no_mysql_dburl):
    with S(no_mysql_dburl, echo=ECHO) as s:
        q = s.query(Light.id).order_by(Light.colour, Light.intensity, Light.id)
        check_paging_orm(q=q)


def test_orm_result_processor(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Light.id, Light.myint).order_by(Light.intensity, Light.id)
        check_paging_orm(q=q)


def test_orm_order_by_result_processor(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Light.id).order_by(Light.myint, Light.id)
        check_paging_orm(q=q)


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
    assert process_args(False, False, None) == (None, False)


def test_bookmarks():
    def twoway(x):
        before = x
        ss = serialize_bookmark(x)
        after = unserialize_bookmark(ss)
        return before == after

    first = (None, False)
    last = (None, True)

    assert serialize_bookmark(first) == ">"
    assert serialize_bookmark(last) == "<"
    assert twoway(first)
    assert twoway(last)


def test_warn_when_sorting_by_nullable(dburl):
    with pytest.warns(UserWarning):
        with S(dburl, echo=ECHO) as s:
            q = s.query(Book).order_by(Book.a, Book.id)
            get_page(q, per_page=10, page=(None, False))


def test_orm_custom_session_bind(dburl):
    spec = [desc(Book.b), Book.d, Book.id]

    with S(dburl, echo=ECHO) as s:
        # this is a hack but it correctly emulates a session with a custom
        # implementation of get_bind() and no .bind attribute:
        s._custom_bind = s.bind
        delattr(s, "bind")
        s.get_bind = lambda *a, **k: s._custom_bind

        q = s.query(Book, Author, Book.id).outerjoin(Author).order_by(*spec)
        check_paging_orm(q=q)


def test_multiple_engines(dburl, joined_inheritance_dburl):

    eng = sqlalchemy.create_engine(dburl)
    eng2 = sqlalchemy.create_engine(joined_inheritance_dburl)
    session_factory = sessionmaker()
    Base.metadata.bind = eng
    JoinedInheritanceBase.metadata.bind = eng2

    s = session_factory()

    spec = [desc(Book.b), Book.d, Book.id]
    q = s.query(Book, Author, Book.id).outerjoin(Author).order_by(*spec)

    check_paging_orm(q=q)

    s.close()
    Base.metadata.bind = None
    JoinedInheritanceBase.metadata.bind = None


def test_marker_and_bookmark_per_item(dburl):

    with S(dburl, echo=ECHO) as s:
        q = s.query(Book).order_by(Book.id)
        page = get_page(q, per_page=3)

        paging = page.paging
        assert len(page) == 3
        assert paging.get_marker_at(0) == ((1,), False)
        assert paging.get_marker_at(1) == ((2,), False)
        assert paging.get_marker_at(2) == ((3,), False)

        paging_items = list(paging.items())
        assert len(paging_items) == 3
        for i, (key, book) in enumerate(paging_items):
            assert key == ((i + 1,), False)
            assert book.id == i + 1

        assert paging.get_bookmark_at(0) == ">i:1"
        assert paging.get_bookmark_at(1) == ">i:2"
        assert paging.get_bookmark_at(2) == ">i:3"

        bookmark_items = list(paging.bookmark_items())
        assert len(bookmark_items) == 3
        for i, (key, book) in enumerate(bookmark_items):
            assert key == ">i:%d" % (i + 1)
            assert book.id == i + 1

        # Test backwards paging without excess
        place = (3,)
        page = get_page(q, per_page=3, before=place)

        paging = page.paging
        assert len(page) == 2
        # *Paging* backwards doesn't mean *sorting* backwards!
        # The page before id=3 should include items in *ascending* order, with
        # the last one having id=2.
        assert paging.get_marker_at(0) == ((1,), True)
        assert paging.get_marker_at(1) == ((2,), True)

        assert paging.get_bookmark_at(0) == "<i:1"
        assert paging.get_bookmark_at(1) == "<i:2"

        bookmark_items = list(paging.bookmark_items())
        assert len(bookmark_items) == 2
        for i, (key, book) in enumerate(bookmark_items):
            assert key == "<i:%d" % (i + 1)
            assert book.id == i + 1

        # Test backwards paging with excess
        place = (10,)
        page = get_page(q, per_page=3, before=place)

        paging = page.paging

        assert len(page) == 3
        assert paging.get_marker_at(0) == ((7,), True)
        assert paging.get_marker_at(1) == ((8,), True)
        assert paging.get_marker_at(2) == ((9,), True)

        assert paging.get_bookmark_at(0) == "<i:7"
        assert paging.get_bookmark_at(1) == "<i:8"
        assert paging.get_bookmark_at(2) == "<i:9"

        bookmark_items = list(paging.bookmark_items())
        assert len(bookmark_items) == 3
        for i, (key, book) in enumerate(bookmark_items):
            assert key == "<i:%d" % (i + 7)
            assert book.id == i + 7
        for i, (key, book) in enumerate(paging.items()):
            assert key == ((i + 7,), True)
            assert book.id == i + 7
