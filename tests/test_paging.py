"""Integration tests that verify sqlakeyset produces correct results when run
against real databases (SQLite, MySQL, PostgreSQL).

To run these, you need MySQL and PostgreSQL servers running locally, and your
user needs passwordless access with permissions to create new databases.

If you don't want to set this up, you can instead install the CircleCI Local
CLI and run e.g.

    circleci local execute build-3.11-2.0.0

which will execute the tests for python 3.11 and sqlalchemy ~=2.0.0 using
docker containers. (Available python versions are 3.7, 3.8, 3.9, 3.10, 3.11 and
valid sqlalchemy versions are 1.3.0, 1.4.0, 2.0.0.)"""
import warnings
from packaging import version

import pytest
import sqlalchemy
from sqlalchemy.orm import Session, sessionmaker, aliased, Bundle
from sqlalchemy import (
    desc,
    func,
)

from sqlakeyset import (
    get_page,
    select_page,
    serialize_bookmark,
    unserialize_bookmark,
    InvalidPage,
)
from sqlakeyset.paging import process_args
from conftest import (
    Book,
    Author,
    ECHO,
    S,
    Animal,
    Vertebrate,
    Mammal,
    Light,
    Widget,
    select,
    JoinedInheritanceBase,
    Base,
    SQLA2,
    SQLA_VERSION,
)

warnings.simplefilter("error")


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


def check_paging_core(selectable, s, unique=False):
    item_counts = range(1, 12)

    if isinstance(s, Session):
        result = s.execute(selectable)
    else:
        result = Session(bind=s).execute(selectable)
    unpaged = result.unique().fetchall() if unique else result.fetchall()

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
            get_page(q, per_page=10, page=((1,), False, "Potatoes"))  # type: ignore

        # one order col, so check place with 2 elements fails
        with pytest.raises(InvalidPage):
            get_page(q, per_page=10, page=((1, 1), False))


@pytest.mark.skipif(
    SQLA_VERSION < version.parse("1.4.0b1"),
    reason="._mapping doesn't exist in sqlalchemy<1.4",
)
def test_orm_row_mapping(dburl):
    with S(dburl, echo=ECHO) as s:
        q = s.query(Book.id, Book.author_id).order_by(Book.name, Book.id)
        orig = q.first()
        new = get_page(q, per_page=2)[0]
        omap = orig._mapping
        nmap = new._mapping
        assert dict(omap) == dict(nmap)


@pytest.mark.skipif(
    SQLA_VERSION < version.parse("1.4.0b1"),
    reason="._mapping doesn't exist in sqlalchemy<1.4",
)
def test_core_row_mapping(dburl):
    with S(dburl, echo=ECHO) as s:
        q = select(Book.id, Book.author_id).order_by(Book.name, Book.id)
        orig = s.execute(q).first()
        new = select_page(s, q, per_page=2)[0]
        omap = orig._mapping
        nmap = new._mapping
        assert dict(omap) == dict(nmap)


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


def test_orm_column_named_info(dburl):
    # See issue djrobstep#24
    with S(dburl, echo=ECHO) as s:
        aa1 = aliased(Author)
        aa2 = aliased(Author)
        q = (
            s.query(aa2)
            .select_from(aa1)
            .join(aa2, aa2.id == aa1.id)
            .order_by(aa1.info, aa1.id)
        )

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
    selectable = (
        select(Book.b, Book.d, Book.id, Book.c)
        .where(Book.d == 99)
        .order_by(Book.b, Book.d, Book.id, Book.c)
    )

    with S(dburl, echo=ECHO) as s:
        check_paging_core(selectable=selectable, s=s)

    # Check again with a connection instead of session (see #37):
    with S(dburl, echo=ECHO) as s:
        check_paging_core(selectable=selectable, s=s.connection())


def test_core2(dburl):
    with S(dburl, echo=ECHO) as s:
        sel = select(Book.score).order_by(Book.id)
        check_paging_core(sel, s)

        sel = (
            select(Book.score)
            .order_by(Author.id - Book.id, Book.id)
            .where(Author.id == Book.author_id)
        )
        check_paging_core(sel, s)

        sel = (
            select(Book.author_id, func.count())
            .group_by(Book.author_id)
            .order_by(func.sum(Book.popularity))
        )
        check_paging_core(sel, s)

        v = func.sum(func.coalesce(Book.a, 0)) + func.min(Book.b)
        sel = (
            select(Book.author_id, func.count(), v).group_by(Book.author_id).order_by(v)
        )
        check_paging_core(sel, s)


def test_core_enum(dburl):
    with S(dburl, echo=ECHO) as s:
        selectable = select(Light.id, Light.colour).order_by(Light.intensity, Light.id)
        check_paging_core(selectable=selectable, s=s)


# MySQL sorts enums by index in ORDER BY clauses, but treats them as
# strings in ROW() constructors, and thus compares them by their labels;
# so we don't test ordering by enums in MySQL.  If users want to do
# this, they need to ensure that their enums are defined in alphabetical
# order (as recommended by the MySQL documentation).
def test_core_order_by_enum(no_mysql_dburl):
    with S(no_mysql_dburl, echo=ECHO) as s:
        selectable = select(Light.id, Light.colour).order_by(
            Light.colour, Light.intensity, Light.id
        )
        check_paging_core(selectable=selectable, s=s)


def test_core_result_processor(dburl):
    with S(dburl, echo=ECHO) as s:
        selectable = select(Light.id, Light.myint).order_by(Light.intensity, Light.id)
        check_paging_core(selectable=selectable, s=s)


def test_core_order_by_result_processor(dburl):
    with S(dburl, echo=ECHO) as s:
        # Check both 1-col and multicol ordering clauses (see #104):
        selectable = select(Light.id).order_by(Light.myint)
        check_paging_core(selectable=selectable, s=s)

        selectable = select(Light.id).order_by(Light.myint, Light.id)
        check_paging_core(selectable=selectable, s=s)


def test_core_non_declarative(dburl):
    with S(dburl, echo=ECHO) as s:
        selectable = select(Widget).order_by(Widget.c.id)
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
        # Can't pass after AND before
        process_args(before=(1, 2), after=(1, 2))

    with pytest.raises(ValueError):
        # Can't pass before AND page
        process_args(before=(1, 2), page=((1, 2), True))

    with pytest.raises(ValueError):
        # Can't pass after AND page
        process_args(after=(1, 2), page=((1, 2), True))

    with pytest.raises(ValueError):
        # page must be a full marker
        process_args(page=(1, 2))  # type: ignore

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
        s.get_bind = lambda *_, **__: s._custom_bind

        q = s.query(Book, Author, Book.id).outerjoin(Author).order_by(*spec)
        check_paging_orm(q=q)


def test_multiple_engines(dburl, joined_inheritance_dburl):
    kw: dict = {"future": True} if SQLA2 else {}
    eng = sqlalchemy.create_engine(dburl, **kw)
    eng2 = sqlalchemy.create_engine(joined_inheritance_dburl, **kw)
    session_factory = sessionmaker(bind=eng, **kw)
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
