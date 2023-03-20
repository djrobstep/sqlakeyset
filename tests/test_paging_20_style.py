import pytest
from packaging import version
from sqlalchemy import desc, func, select
from sqlalchemy.orm import aliased
from test_paging import (
    ECHO,
    SQLA_VERSION,
    Author,
    Book,
    S,
    check_paging_core,
    dburl,
    pg_only_dburl,
)

if SQLA_VERSION < version.parse("1.4.0b1"):
    pytest.skip(
        "Legacy SQLAlchemy version, skipping new-style tests", allow_module_level=True
    )


#### SQLAlchemy 2-style ORM tests ####


def test_new_orm_query1(dburl):
    spec = [desc(Book.b), Book.d, Book.id]

    with S(dburl, echo=ECHO) as s:
        q = select(Book, Author, Book.id).outerjoin(Author).order_by(*spec)
        check_paging_core(q, s)


def test_new_orm_query2(dburl):
    with S(dburl, echo=ECHO) as s:
        q = select(Book).order_by(Book.id, Book.name)
        check_paging_core(q, s)


def test_new_orm_query3(dburl):
    with S(dburl, echo=ECHO) as s:
        q = select(Book.id, Book.name.label("x")).order_by(Book.name, Book.id)
        check_paging_core(q, s)


def test_new_orm_query4(dburl):
    with S(dburl, echo=ECHO) as s:
        q = select(Book).order_by(Book.name)
        check_paging_core(q, s)


def test_new_orm_order_by_arrowtype(dburl):
    with S(dburl, echo=ECHO) as s:
        q = select(Book).order_by(Book.published_at, Book.id)
        check_paging_core(q, s)


def test_new_orm_implicit_join(dburl):
    with S(dburl, echo=ECHO) as s:
        q = (
            select(Book)
            .order_by(Author.name, Book.id)
            .filter(Book.author_id == Author.id)
        )
        check_paging_core(q, s)


def test_new_orm_hybrid_property(dburl):
    with S(dburl, echo=ECHO) as s:
        q = select(Book).order_by(Book.score, Book.id)
        check_paging_core(q, s)
        q = select(Book, Author).join(Book.author).order_by(Book.score, Book.id)
        check_paging_core(q, s)
        q = (
            select(Book.score, Book, Author)
            .join(Book.author)
            .order_by(Book.score, Book.id)
        )
        check_paging_core(q, s)


def test_new_orm_column_property(dburl):
    with S(dburl, echo=ECHO) as s:
        q = select(Book).order_by(Book.popularity, Book.id)
        check_paging_core(q, s)
        q = (
            select(Book, Author)
            .join(Book.author)
            .order_by(Book.popularity.desc(), Book.id)
        )
        check_paging_core(q, s)


def test_new_orm_column_named_info(dburl):
    # See issue djrobstep#24
    with S(dburl, echo=ECHO) as s:
        aa1 = aliased(Author)
        aa2 = aliased(Author)
        q = (
            select(aa2)
            .select_from(aa1)
            .join(aa2, aa2.id == aa1.id)
            .order_by(aa1.info, aa1.id)
        )

        check_paging_core(q, s)


def test_new_orm_correlated_subquery_hybrid(dburl):
    with S(dburl, echo=ECHO) as s:
        q = select(Author.id, Author.name).order_by(Author.book_count, Author.id)
        check_paging_core(q, s)


def test_new_orm_expression(dburl):
    with S(dburl, echo=ECHO) as s:
        key = func.coalesce(Book.a, 0) + Book.b
        q = select(Book).order_by(key, Book.id)
        check_paging_core(q, s)

        q = select(Book).order_by(key.label("sort_by_me"), Book.id)
        check_paging_core(q, s)


def test_new_orm_aggregated(dburl):
    count = func.count().label("count")
    spec = [desc(count), desc(Author.name), Author.id]

    with S(dburl, echo=ECHO) as s:
        q = select(Author, count).join(Author.books).group_by(Author).order_by(*spec)
        check_paging_core(q, s)


def test_new_orm_subquery(dburl):
    count = func.count().label("count")

    with S(dburl, echo=ECHO) as s:
        sq = (
            select(Author.id, count)
            .join(Author.books)
            .group_by(Author.id)
            .subquery("sq")
        )
        q = (
            select(sq.c.count, Author)
            .join(sq, sq.c.id == Author.id)
            .order_by(desc(sq.c.count), Author.name, Author.id)
        )
        check_paging_core(q, s)


def test_new_orm_recursive_cte(pg_only_dburl):
    with S(pg_only_dburl, echo=ECHO) as s:
        # Start with "origins": books that don't have prequels
        seed = select(Book.id.label("id"), Book.id.label("origin")).filter(
            Book.prequel is None
        )

        # Recurse by picking up sequels
        sequel = aliased(Book, name="sequel")
        recursive = seed.cte(recursive=True)
        recursive = recursive.union(
            select(sequel.id, recursive.c.origin).filter(
                sequel.prequel_id == recursive.c.id
            )
        )

        # Count total books per origin
        count = func.count().label("count")
        origin = recursive.c.origin.label("origin")
        sq = select(origin, count).group_by(origin).cte(recursive=False)

        # Join to full book table
        q = (
            select(sq.c.count, Book)
            .filter(Book.id == sq.c.origin)
            .order_by(sq.c.count.desc(), Book.id)
        )

        check_paging_core(q, s)
