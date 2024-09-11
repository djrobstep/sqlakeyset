import enum
import uuid
from datetime import timedelta
from functools import partial
from random import randrange

import arrow
import pytest
from packaging import version
from sqlalchemy import (
    Column,
    Enum,
    ForeignKey,
    Integer,
    String,
    Table,
    func,
    insert,
    inspect,
)
from sqlalchemy import select as _select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import column_property, relationship
from sqlalchemy.types import TypeDecorator
from sqlalchemy_utils import ArrowType
from sqlbag import S as _S
from sqlbag import temporary_database

from sqlakeyset import custom_bookmark_type
from sqlakeyset.sqla import SQLA_VERSION

SQLA2 = SQLA_VERSION >= version.parse("1.4")
if not SQLA2:
    # This block needs to be before the sqla2 block for type hints to work correctly??
    # Thus the backwards if-else.
    from sqlalchemy.ext.declarative import declarative_base

    select = lambda *args: _select(args)
    S = _S
else:
    from sqlalchemy.orm import declarative_base

    select = _select
    S = partial(_S, future=True)

if SQLA_VERSION >= version.parse("2.0"):
    from sqlalchemy.types import Uuid
else:
    # On older sqlalchemy we don't run the uuid tests anyway, so this is good enough
    class Uuid(TypeDecorator):
        impl = String
        cache_ok = True

        def process_result_value(self, value, dialect):
            return uuid.UUID(hex=value) if value is not None else None

        def process_bind_param(self, value, dialect):
            return value.hex if value is not None else None

        def __init__(self):
            super().__init__(32)


class Base(declarative_base()):
    __abstract__ = True

    def _as_dict(self):
        return {k: getattr(self, k) for k, v in self.__mapper__.columns.items()}

    def __eq__(self, other):
        return type(self) is type(other) and self._as_dict() == other._as_dict()

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
class MyInteger:
    val: int

    def __init__(self, val: int):
        self.val = val

    def __eq__(self, other):
        if isinstance(other, MyInteger):
            return self.val == other.val
        return other == self  # for SQL

    def __hash__(self):
        return hash(self.val)

    def __str__(self):
        return str(self.val)

    def __repr__(self):
        return f"MyInteger({self.val})"


custom_bookmark_type(
    MyInteger,
    "mi",
    serializer=lambda mi: str(mi.val),
    deserializer=lambda s: MyInteger(int(s)),
)


class DoubleProcessing(Exception):
    pass


class GuardDoubleProcessing(TypeDecorator):
    impl = Integer
    cache_ok = True

    def process_result_value(self, value, dialect):
        if isinstance(value, MyInteger):
            raise DoubleProcessing(
                f"Result processor was called on an already processed value: {value!r}"
            )
        assert isinstance(value, int)
        return MyInteger(value)

    def process_bind_param(self, value, dialect):
        if isinstance(value, int):
            raise DoubleProcessing(
                f"Bind processor was called on an already processed value: {value!r} is not a MyInteger"
            )
        return value.val


class EnforceDialectSpecificTypes(TypeDecorator):
    class DialectSpecificImpl(Exception):
        pass

    class InvalidTypeEngine(String):
        def bind_processor(self, dialect):
            raise EnforceDialectSpecificTypes.DialectSpecificImpl(
                "Did not get the dialect specific impl."
            )

    impl = InvalidTypeEngine
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(String(255))


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
    myint = Column(GuardDoubleProcessing, nullable=False)


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
    name = Column(EnforceDialectSpecificTypes, nullable=False)
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


Widget = Table(
    "widget",
    Base.metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(255)),
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


class Ticket(Base):
    __tablename__ = "ticket"
    id = Column(Uuid, primary_key=True)


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
        Light(colour=Colour(i % 3), intensity=(i * 13) % 53, myint=MyInteger(i))
        for i in range(99)
    ]

    widgets = [dict(name=f"widget {i}") for i in range(99)]

    data += [Ticket(id=uuid.uuid4()) for _ in range(100)]

    with temporary_database(request.param, host="localhost") as dburl:
        with S(dburl) as s:
            if request.param == "postgresql":
                tables = None
            else:
                tables = [
                    t
                    for k, t in Base.metadata.tables.items()
                    if not k.startswith("pg_only_")
                ]
            Base.metadata.create_all(s.connection(), tables=tables)
            s.add_all(data)
            s.execute(insert(Widget).values(widgets))
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
