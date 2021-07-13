"""Main paging interface and implementation."""

from functools import partial

from sqlalchemy import tuple_

from .columns import find_order_key, parse_ob_clause
from .results import Page, Paging, unserialize_bookmark
from .serial import InvalidPage
from .sqla import (
    core_coerce_row,
    core_result_type,
    group_by_clauses,
    orm_coerce_row,
    orm_query_keys,
    orm_result_type,
    orm_to_selectable,
)

PER_PAGE_DEFAULT = 10


def where_condition_for_page(ordering_columns, place, dialect):
    """Construct the SQL condition required to restrict a query to the desired
    page.

    :param ordering_columns: The query's ordering columns
    :type ordering_columns: list(:class:`.columns.OC`)
    :param place: The starting position for the page
    :type place: tuple
    :param dialect: The SQL dialect in use
    :returns: An SQLAlchemy expression suitable for use in ``.where()`` or
        ``.filter()``.
    """
    if len(ordering_columns) != len(place):
        raise InvalidPage(
            "Page marker has different column count to query's order clause"
        )

    zipped = zip(ordering_columns, place)
    swapped = [c.pair_for_comparison(value, dialect) for c, value in zipped]
    row, place_row = zip(*swapped)

    if len(row) == 1:
        condition = row[0] > place_row[0]
    else:
        condition = tuple_(*row) > tuple_(*place_row)
    return condition


def orm_page_from_rows(
    paging_result, result_type, page_size, backwards=False, current_marker=None
):
    """Turn a raw page of results for an ORM query (as obtained by
    :func:`orm_get_page`) into a :class:`.results.Page` for external
    consumers."""

    ocols, mapped_ocols, extra_columns, rows, keys = paging_result

    make_row = partial(
        orm_coerce_row, extra_columns=extra_columns, result_type=result_type
    )
    out_rows = [make_row(row) for row in rows]
    key_rows = [tuple(col.get_from_row(row) for col in mapped_ocols) for row in rows]
    paging = Paging(
        out_rows, page_size, ocols, backwards, current_marker, markers=key_rows
    )

    page = Page(paging.rows, paging, keys=keys)
    return page


def perform_paging(q, per_page, place, backwards, orm=True, s=None):
    if orm:
        selectable = orm_to_selectable(q)
        s = q.session
        column_descriptions = q.column_descriptions
        keys = orm_query_keys(q)
    else:
        if not s:
            raise ValueError("Cannot page core selectable without a session/connection")
        selectable = q
        column_descriptions = q._raw_columns
    try:
        # for sessions, dialect is available via the bind:
        dialect = s.get_bind().dialect
    except Exception:
        # connections have a direct .dialect
        dialect = s.dialect

    order_cols = parse_ob_clause(selectable)
    if backwards:
        order_cols = [c.reversed for c in order_cols]
    mapped_ocols = [find_order_key(ocol, column_descriptions) for ocol in order_cols]

    clauses = [col.ob_clause for col in mapped_ocols]
    q = q.order_by(None).order_by(*clauses)
    if orm:
        q = q.only_return_tuples(True)

    extra_columns = [
        col.extra_column for col in mapped_ocols if col.extra_column is not None
    ]
    if hasattr(q, "add_columns"):  # ORM or SQLAlchemy 1.4+
        q = q.add_columns(*extra_columns)
    else:
        for col in extra_columns:  # SQLAlchemy Core <1.4
            q = q.column(col)

    if place:
        condition = where_condition_for_page(order_cols, place, dialect)
        # For aggregate queries, paging condition is applied *after*
        # aggregation. In SQL this means we need to use HAVING instead of
        # WHERE.
        groupby = group_by_clauses(selectable)
        if groupby is not None and len(groupby) > 0:
            q = q.having(condition)
        elif orm:
            q = q.filter(condition)
        else:
            q = q.where(condition)

    q = q.limit(per_page + 1)  # 1 extra to check if there's a further page
    if orm:
        rows = q.all()
    else:
        selected = s.execute(q)
        keys = list(selected.keys())
        N = len(keys) - len(extra_columns)
        keys = keys[:N]
        rows = selected.fetchall()
    return order_cols, mapped_ocols, extra_columns, rows, keys


def orm_get_page(q, per_page, place, backwards):
    """Get a page from an SQLAlchemy ORM query.

    :param q: The :class:`Query` to paginate.
    :param per_page: Number of rows per page.
    :param place: Keyset representing the place after which to start the page.
    :param backwards: If ``True``, reverse pagination direction.
    :returns: :class:`Page`
    """
    result_type = orm_result_type(q)
    paging_result = perform_paging(
        q=q, per_page=per_page, place=place, backwards=backwards, orm=True
    )
    page = orm_page_from_rows(
        paging_result, result_type, per_page, backwards, current_marker=place
    )
    return page


def core_get_page(s, selectable, per_page, place, backwards):
    """Get a page from an SQLAlchemy Core selectable.

    :param s: :class:`sqlalchemy.engine.Connection` or
        :class:`sqlalchemy.orm.session.Session` to use to execute the query.
    :param selectable: The source selectable.
    :param per_page: Number of rows per page.
    :param place: Keyset representing the place after which to start the page.
    :param backwards: If ``True``, reverse pagination direction.
    :returns: :class:`Page`
    """
    # We need the result schema for the *original* query in order to properly
    # trim off our extra_columns. As far as I can tell, this is the only
    # way to get it without copy-pasting chunks of the sqlalchemy internals.
    # LIMIT 0 to minimize database load (though the fact that a round trip to
    # the DB has to happen at all is regrettable).
    result_type = core_result_type(selectable, s)
    paging_result = perform_paging(
        q=selectable,
        per_page=per_page,
        place=place,
        backwards=backwards,
        orm=False,
        s=s,
    )
    page = core_page_from_rows(
        paging_result, result_type, per_page, backwards, current_marker=place
    )
    return page


def core_page_from_rows(
    paging_result, result_type, page_size, backwards=False, current_marker=None
):
    """Turn a raw page of results for an SQLAlchemy Core query (as obtained by
    :func:`.core_get_page`) into a :class:`.Page` for external consumers."""
    ocols, mapped_ocols, extra_columns, rows, keys = paging_result

    make_row = partial(
        core_coerce_row, extra_columns=extra_columns, result_type=result_type
    )
    out_rows = [make_row(row) for row in rows]
    key_rows = [tuple(col.get_from_row(row) for col in mapped_ocols) for row in rows]
    paging = Paging(
        out_rows, page_size, ocols, backwards, current_marker, markers=key_rows
    )
    page = Page(paging.rows, paging, keys=keys)
    return page


def process_args(after=False, before=False, page=None):
    if isinstance(page, str):
        page = unserialize_bookmark(page)

    if before is not False and after is not False:
        raise ValueError("after *OR* before")

    if (before is not False or after is not False) and page is not None:
        raise ValueError("specify either a page tuple, or before/after")

    if page:
        try:
            place, backwards = page
        except ValueError as e:
            raise InvalidPage("page is not a recognized string or tuple") from e
    elif after:
        place = after
        backwards = False
    elif before:
        place = before
        backwards = True
    else:
        backwards = False
        place = None

    return place, backwards


def select_page(
    s, selectable, per_page=PER_PAGE_DEFAULT, after=False, before=False, page=None
):
    """Get a page of results from a SQLAlchemy Core selectable.

    Specify no more than one of the arguments ``page``, ``after`` or
    ``before``. If none of these are provided, the first page is returned.

    :param s: :class:`sqlalchemy.engine.Connection` or
        :class:`sqlalchemy.orm.session.Session` to use to execute the query.
    :param selectable: The source selectable.
    :param per_page: The (maximum) number of rows on the page.
    :type per_page: int, optional.
    :param page: a ``(keyset, backwards)`` pair or string bookmark describing
        the page to get.
    :param after: if provided, the page will consist of the rows immediately
        following the specified keyset.
    :param before: if provided, the page will consist of the rows immediately
        preceding the specified keyset.

    :returns: A :class:`Page` containing the requested rows and paging hooks
        to access surrounding pages.
    """
    place, backwards = process_args(after, before, page)

    return core_get_page(s, selectable, per_page, place, backwards)


def get_page(query, per_page=PER_PAGE_DEFAULT, after=False, before=False, page=None):
    """Get a page of results for an ORM query.

    Specify no more than one of the arguments ``page``, ``after`` or
    ``before``. If none of these are provided, the first page is returned.

    :param query: The source query.
    :type query: :class:`sqlalchemy.orm.query.Query`.
    :param per_page: The (maximum) number of rows on the page.
    :type per_page: int, optional.
    :param page: a ``(keyset, backwards)`` pair or string bookmark describing
        the page to get.
    :param after: if provided, the page will consist of the rows immediately
        following the specified keyset.
    :param before: if provided, the page will consist of the rows immediately
        preceding the specified keyset.

    :returns: A :class:`Page` containing the requested rows and paging hooks
        to access surrounding pages.
    """
    place, backwards = process_args(after, before, page)

    return orm_get_page(query, per_page, place, backwards)
