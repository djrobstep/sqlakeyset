"""Main paging interface and implementation."""
from copy import copy

from sqlalchemy import func
from sqlalchemy.engine.result import RowProxy as _RowProxy
from sqlalchemy.orm.query import Query
from sqlalchemy.util import lightweight_named_tuple

from .columns import parse_clause, find_order_key
from .results import Page, Paging, unserialize_bookmark

PER_PAGE_DEFAULT = 10

def orm_result_type(query):
    """Return the type constructor for rows that would be returned by a given
    query; or the identity function for queries that return a single entity.

    :param query: The query to inspect.
    :type query: :class:`sqlalchemy.orm.query.Query`.
    :returns: either a named tuple type or the identity."""

    if query.is_single_entity:
        return lambda x: x[0]
    labels = [e._label_name for e in query._entities]
    return lightweight_named_tuple("result", labels)


def where_condition_for_page(ordering_columns, place):
    """Construct the SQL condition required to restrict a query to the desired
    page.

    :param ordering_columns: The query's ordering columns
    :type ordering_columns: list(:class:`.columns.OC`)
    :param place: The starting position for the page
    :type place: tuple
    :returns: An SQLAlchemy expression suitable for use in ``.where()`` or
        ``.filter()``.
    """
    row, place_row = paging_condition(ordering_columns, place)
    if len(row) == 1:
        condition = row[0] > place_row[0]
    else:
        condition = func.row(*row) > func.row(*place_row)
    return condition


def orm_page_from_rows(paging_result,
                       result_type,
                       page_size,
                       backwards=False,
                       current_marker=None):
    """Turn a raw page of results for an ORM query (as obtained by
    :func:`orm_get_page`) into a :class:`.results.Page` for external
    consumers."""

    ocols, mapped_ocols, extra_entities, rows, keys = paging_result

    # orm_get_page might have added some extra columns to the query in order
    # to get the keys for the bookmark. Here, we split the rows back to the
    # requested data and the ordering keys.
    def clean_row(row):
        """Trim off the extra entities."""
        N = len(row) - len(extra_entities)
        return result_type(row[:N])

    out_rows = [clean_row(row) for row in rows]
    key_rows = [tuple(col.get_from_row(row) for col in mapped_ocols)
                for row in rows]
    paging = Paging(out_rows, page_size, ocols, backwards,
                    current_marker, markers=key_rows)

    page = Page(paging.rows, paging, keys=keys)
    return page


def perform_paging(q, per_page, place, backwards, orm=True, s=None):
    if orm:
        selectable = q.selectable
        column_descriptions = q.column_descriptions
        keys = [e._label_name for e in q._entities]
    else:
        selectable = q
        column_descriptions = q._raw_columns

    ob_clause = selectable._order_by_clause
    order_cols = parse_clause(ob_clause)
    if backwards:
        order_cols = [c.reversed for c in order_cols]
    mapped_ocols = [find_order_key(ocol, column_descriptions)
                    for ocol in order_cols]

    clauses = [col.ob_clause for col in mapped_ocols]
    q = q.order_by(None).order_by(*clauses)
    if orm:
        q = q.only_return_tuples(True)

    extra_entities = [col.extra_entity for col in mapped_ocols
                      if col.extra_entity is not None]
    if extra_entities:
        if orm:
            existing_entities = (e.expr for e in q._entities)
            q = q.with_entities(*existing_entities, *extra_entities)
        else:
            for e in extra_entities:
                q.append_column(e)

    if place:
        condition = where_condition_for_page(order_cols, place)
        # For aggregate queries, paging condition is applied *after*
        # aggregation. In SQL this means we need to use HAVING instead of
        # WHERE.
        groupby = selectable._group_by_clause
        if groupby is not None and len(groupby) > 0:
            q = q.having(condition)
        elif orm:
            q = q.filter(condition)
        else:
            q = q.where(condition)

    q = q.limit(per_page + 1) # 1 extra to check if there's a further page
    if orm:
        rows = q.all()
    else:
        selected = s.execute(q)
        keys = selected.keys()
        N = len(keys) - len(extra_entities)
        keys = keys[:N]
        rows = selected.fetchall()
    return order_cols, mapped_ocols, extra_entities, rows, keys

def orm_get_page(q, per_page, place, backwards):
    """Get a page from an SQLAlchemy ORM query.

    :param q: The :class:`Query` to paginate.
    :param per_page: Number of rows per page.
    :param place: Keyset representing the place after which to start the page.
    :param backwards: If ``True``, reverse pagination direction.
    :returns: :class:`Page`
    """
    result_type = orm_result_type(q)
    paging_result = perform_paging(q=q,
                                   per_page=per_page,
                                   place=place,
                                   backwards=backwards,
                                   orm=True)
    page = orm_page_from_rows(paging_result,
                              result_type,
                              per_page,
                              backwards,
                              current_marker=place)

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
    # trim off our extra_entities. As far as I can tell, this is the only
    # way to get it. LIMIT 0 to minimize database load (though the fact that a
    # round trip to the DB has to happen at all is regrettable).
    result_proxy = s.execute(selectable.limit(0))
    paging_result = perform_paging(q=selectable,
                                   per_page=per_page,
                                   place=place,
                                   backwards=backwards,
                                   orm=False,
                                   s=s)
    page = core_page_from_rows(paging_result,
                               result_proxy,
                               per_page,
                               backwards,
                               current_marker=place)
    return page


def core_page_from_rows(
        paging_result,
        result_proxy,
        page_size,
        backwards=False,
        current_marker=None):
    """Turn a raw page of results for an SQLAlchemy Core query (as obtained by
    :func:`.core_get_page`) into a :class:`.Page` for external consumers."""
    ocols, mapped_ocols, extra_columns, rows, keys = paging_result

    def clean_row(row):
        """Trim off the extra columns and return as a correct-as-possible
        RowProxy."""
        if not extra_columns:
            return row
        N = len(row._row) - len(extra_columns)
        row = row[:N]
        process_row = result_proxy._process_row
        metadata = result_proxy._metadata
        keymap = metadata._keymap
        processors = metadata._processors
        return process_row(metadata, row, processors, keymap)

    out_rows = [clean_row(row) for row in rows]
    key_rows = [tuple(col.get_from_row(row) for col in mapped_ocols)
                for row in rows]
    paging = Paging(out_rows, page_size, ocols, backwards,
                    current_marker, markers=key_rows)
    page = Page(paging.rows, paging, keys=keys)
    return page


def paging_condition(ordering_columns, place):
    if len(ordering_columns) != len(place):
        raise ValueError('bad paging value') # pragma: no cover

    def swapped_if_descending(c, value):
        if not c.is_ascending:
            return value, c.comparable_value
        else:
            return c.comparable_value, value

    zipped = zip(ordering_columns, place)
    swapped = [swapped_if_descending(c, value) for c, value in zipped]
    row, place_row = zip(*swapped)
    return row, place_row


def process_args(after=False, before=False, page=False):
    if isinstance(page, str):
        page = unserialize_bookmark(page)

    if before is not False and after is not False:
        raise ValueError('after *OR* before')

    if (before is not False or after is not False) and page is not False:
        raise ValueError('specify either a page tuple, or before/after')

    if page:
        place, backwards = page
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
        s,
        selectable,
        per_page=PER_PAGE_DEFAULT,
        after=False,
        before=False,
        page=False):
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

    return core_get_page(
        s,
        selectable,
        per_page,
        place,
        backwards)


def get_page(
        query,
        per_page=PER_PAGE_DEFAULT,
        after=False, before=False,
        page=False):
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

    return orm_get_page(
        query,
        per_page,
        place,
        backwards)
