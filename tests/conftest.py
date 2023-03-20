import enum
from datetime import timedelta
from functools import partial
from random import randrange

import arrow
import pytest
from packaging import version
from sqlalchemy import Column, Enum, ForeignKey, Integer, String, func, inspect
from sqlalchemy import select as _select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import column_property, relationship
from sqlalchemy.types import TypeDecorator
from sqlbag import S as _S
from sqlbag import temporary_database

from sqlakeyset.sqla import SQLA_VERSION
from sqlakeyset import custom_bookmark_type

from sqlalchemy_utils import ArrowType

SQLA2 = SQLA_VERSION >= version.parse("1.4")

if SQLA2:
    from sqlalchemy.orm import declarative_base

    select = _select
    S = partial(_S, future=True)
else:
    from sqlalchemy.ext.declarative import declarative_base

    def select(*args):
        return _select(args)

    S = _S


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
            select(func.count(Book.id))
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


SUPPORTED_ENGINES = ["sqlite", "postgresql", "mysql"]

dburl = pytest.fixture(params=SUPPORTED_ENGINES)(_dburl)
no_mysql_dburl = pytest.fixture(params=["sqlite", "postgresql"])(_dburl)
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
