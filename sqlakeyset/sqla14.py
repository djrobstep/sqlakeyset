"""Methods for messing with the internals of SQLAlchemy >1.3 results."""
from sqlalchemy.engine.result import ScalarResult


def orm_query_keys(query):
    """Given a SQLAlchemy ORM query, extract the list of column keys expected
    in the result."""
    return query._iter()._metadata.keys


def orm_result_type(query):
    """Return the type constructor for rows that would be returned by a given
    query; or the identity function for queries that return a single entity
    rather than rows.

    :param query: The query to inspect.
    :type query: :class:`sqlalchemy.orm.query.Query`.
    :returns: either a named tuple type or the identity."""

    _iter = query._iter()
    if isinstance(_iter, ScalarResult):
        return lambda x: x[0]
    return _iter._row_getter


def orm_coerce_row(row, extra_columns, result_type):
    """Trim off the extra columns."""
    # orm_get_page might have added some extra columns to the query in order
    # to get the keys for the bookmark. Here, we split the rows back to the
    # requested data and the ordering keys.
    N = len(row) - len(extra_columns)
    return result_type(row[:N])


def core_result_type(selectable, s):
    """Given a SQLAlchemy Core selectable and a connection/session, get the
    type constructor for the result row type."""
    result_proxy = s.execute(selectable.limit(0))
    return result_proxy._row_getter


def core_coerce_row(row, extra_columns, result_type):
    """Trim off the extra columns and return as a correct-as-possible
    sqlalchemy Row."""
    if not extra_columns:
        return row
    N = len(row) - len(extra_columns)
    return result_type(row[:N])


def orm_to_selectable(q):
    """Normalize an ORM query into a selectable.

    In SQLAlchemy 1.4, there is no distinction."""
    return q


def order_by_clauses(selectable):
    """Extract the ORDER BY clause list from a select/query"""
    return selectable._order_by_clauses


def group_by_clauses(selectable):
    """Extract the GROUP BY clause list from a select/query"""
    return selectable._group_by_clauses
