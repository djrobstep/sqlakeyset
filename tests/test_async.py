import json
import re

import pytest
import pytest_asyncio
from conftest import SQLA_VERSION, Author, Base, Book, Ticket
from packaging import version
from sqlalchemy import Column, Integer, desc, orm, select
from sqlalchemy.types import UserDefinedType

from sqlakeyset.results import (
    custom_bookmark_type,
    serialize_bookmark,
    unserialize_bookmark,
)

if SQLA_VERSION < version.parse("1.4.0"):
    pytest.skip(
        "Legacy SQLAlchemy version, skipping async tests", allow_module_level=True
    )
asa = pytest.importorskip("sqlalchemy.ext.asyncio")
asaks = pytest.importorskip("sqlakeyset.asyncio")


ASYNC_PROTOS = {
    r"postgresql:": "postgresql+asyncpg:",
    r"mysql:": "mysql+aiomysql:",
    r"mysql\+pymysql:": "mysql+aiomysql:",
    r"sqlite:": "sqlite+aiosqlite:",
}


class StringInt(UserDefinedType):
    cache_ok = True

    def bind_processor(self, dialect):
        def process(value):
            return str(value)

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value

        return process

    def get_col_spec(self, **kw):
        return "INT"


class Wat(Base):
    __tablename__ = "wat"

    id = Column(Integer, primary_key=True)
    num = Column(StringInt, nullable=False)


custom_bookmark_type(dict, "D", json.loads, json.dumps)


@pytest_asyncio.fixture
async def async_session(dburl):
    for k, v in ASYNC_PROTOS.items():
        dburl = re.sub("^" + k, v, dburl)
    engine = asa.create_async_engine(dburl, future=True)
    try:
        sessionmaker = asa.async_sessionmaker(engine)
    except AttributeError:  # sqlalchemy 1.4 has no async_sessionmaker
        sessionmaker = orm.sessionmaker(engine, class_=asa.AsyncSession)

    async with sessionmaker() as s:
        s.add_all([Wat(num=i % 3) for i in range(20)])
        yield s


async def check_paging_async(selectable, s):
    item_counts = range(1, 12)

    result = await s.execute(selectable)
    unpaged = result.fetchall()

    for backwards in [False, True]:
        for per_page in item_counts:
            gathered = []

            page = None, backwards

            while True:
                serialized_page = serialize_bookmark(page)
                page = unserialize_bookmark(serialized_page)

                page_with_paging = await asaks.select_page(
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


@pytest.mark.asyncio
async def test_async_orm_query1(async_session):
    spec = [desc(Book.b), Book.d, Book.id]
    q = select(Book, Author, Book.id).outerjoin(Author).order_by(*spec)
    await check_paging_async(q, async_session)


@pytest.mark.asyncio
async def test_uuid(async_session):
    q = select(Ticket).order_by(Ticket.id)
    await check_paging_async(q, async_session)


@pytest.mark.asyncio
async def test_bind_processor(async_session):
    q = select(Wat).order_by(Wat.num, Wat.id)
    await check_paging_async(q, async_session)
