"""Main paging interface and implementation."""

from __future__ import annotations

from functools import partial
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Generic,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    overload,
)
from typing_extensions import Literal  # to keep python 3.7 support

from sqlalchemy import tuple_, and_, or_, func, text
from sqlalchemy.engine import Connection
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.orm import Session
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.expression import ColumnElement, literal, select, union_all
from sqlalchemy.sql.selectable import Select

from .columns import OC, MappedOrderColumn, find_order_key, parse_ob_clause
from .results import Page, Paging, unserialize_bookmark
from .serial import InvalidPage
from .sqla import (
    core_coerce_row,
    core_result_type,
    get_bind,
    group_by_clauses,
    orm_coerce_row,
    orm_query_keys,
    orm_result_type,
    orm_to_selectable,
    Row,
)
from .types import Keyset, Marker, MarkerLike


_TP = TypeVar("_TP", bound=Tuple[Any, ...])

PER_PAGE_DEFAULT = 10

# Dialects built-in to sqlalchemy that support native tuple comparison.
# Other custom dialects may support this too, but we err on the side of
# breaking less.
SUPPORTS_NATIVE_TUPLE_COMPARISON = ("postgresql", "mysql", "sqlite")


def compare_tuples(
    lesser: Sequence, greater: Sequence, dialect: Optional[Dialect] = None
) -> ColumnElement[bool]:
    """Given two sequences of equal length (whose entries can be SQL clauses or
    simple values), create an SQL clause defining the lexicographic tuple
    comparison ``lesser < greater``.

    If ``dialect`` is provided and is an sqlalchemy SQL dialect supporting
    native tuple comparison, the SQL emitted is a native tuple comparison.
    Otherwise it is built manually using OR and AND."""
    if len(lesser) != len(greater):
        raise ValueError("Tuples must have same length to be compared!")
    if len(lesser) == 1:
        return lesser[0] < greater[0]
    if dialect is not None and dialect.name.lower() in SUPPORTS_NATIVE_TUPLE_COMPARISON:
        return tuple_(*lesser) < tuple_(*greater)
    return or_(
        *[
            and_(
                *[lesser[index] == greater[index] for index in range(eq_depth)],
                lesser[eq_depth] < greater[eq_depth],
            )
            for eq_depth in range(len(lesser))
        ]
    )


def where_condition_for_page(
    ordering_columns: List[OC], place: Keyset, dialect: Dialect
) -> ColumnElement[bool]:
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

    return compare_tuples(greater=row, lesser=place_row, dialect=dialect)


class _PagingQuery(NamedTuple):
    query: Query
    order_columns: List[OC]
    mapped_order_columns: List[MappedOrderColumn]
    extra_columns: List


class _PagingSelect(NamedTuple):
    select: Select
    order_columns: List[OC]
    mapped_order_columns: List[MappedOrderColumn]
    extra_columns: List


def orm_page_from_rows(
    paging_query: _PagingQuery,
    rows: Sequence[Row],
    keys: List[str],
    result_type,
    page_size: int,
    backwards: bool = False,
    current_place: Optional[Keyset] = None,
) -> Page:
    """Turn a raw page of results for an ORM query (as obtained by
    :func:`orm_get_page`) into a :class:`.results.Page` for external
    consumers."""

    _, _, mapped_ocols, extra_columns = paging_query

    make_row = partial(
        orm_coerce_row, extra_columns=extra_columns, result_type=result_type
    )
    out_rows = [make_row(row) for row in rows]
    key_rows = [tuple(col.get_from_row(row) for col in mapped_ocols) for row in rows]
    paging = Paging(out_rows, page_size, backwards, current_place, places=key_rows)

    page = Page(paging.rows, paging, keys=keys)
    return page


@overload
def prepare_paging(
    q: Query,
    per_page: int,
    place: Optional[Keyset],
    backwards: bool,
    orm: Literal[True],
    dialect: Dialect,
    page_identifier: Optional[int] = None,
) -> _PagingQuery:
    ...


@overload
def prepare_paging(
    q: Select,
    per_page: int,
    place: Optional[Keyset],
    backwards: bool,
    orm: Literal[False],
    dialect: Dialect,
    page_identifier: Optional[int] = None,
) -> _PagingSelect:
    ...


def prepare_paging(
    q: Union[Query, Select],
    per_page: int,
    place: Optional[Keyset],
    backwards: bool,
    orm: bool,
    dialect: Dialect,
    page_identifier: Optional[int] = None,
) -> Union[_PagingQuery, _PagingSelect]:
    if orm:
        if not isinstance(q, Query):
            raise ValueError("If orm=True then q must be a Query")
        selectable = orm_to_selectable(q)
        column_descriptions = q.column_descriptions
    else:
        if isinstance(q, Query):
            raise ValueError("If orm=False then q cannot be a Query")
        selectable = q
        try:
            column_descriptions = q.column_descriptions
        except Exception:
            column_descriptions = q._raw_columns  # type: ignore

    order_cols = parse_ob_clause(selectable)
    if backwards:
        order_cols = [c.reversed for c in order_cols]
    mapped_ocols = [find_order_key(ocol, column_descriptions) for ocol in order_cols]

    clauses = [col.ob_clause for col in mapped_ocols]
    q = q.order_by(None).order_by(*clauses)
    if orm:
        q = q.only_return_tuples(True)  # type: ignore

    extra_columns = [
        col.extra_column for col in mapped_ocols if col.extra_column is not None
    ]

    if hasattr(q, "add_columns"):  # ORM or SQLAlchemy 1.4+
        q = q.add_columns(*extra_columns)
    else:
        for col in extra_columns:  # SQLAlchemy Core <1.4
            q = q.column(col)  # type: ignore

    q = _apply_where_and_limit(q, selectable, per_page, place, dialect, order_cols, orm)

    if orm:
        assert isinstance(q, Query)
        return _PagingQuery(q, order_cols, mapped_ocols, extra_columns)
    else:
        assert not isinstance(q, Query)
        return _PagingSelect(q, order_cols, mapped_ocols, extra_columns)


def _apply_where_and_limit(q, selectable, per_page, place, dialect, order_cols, orm):
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
    return q


def orm_get_page(
    q: Query[_TP], per_page: int, place: Optional[Keyset], backwards: bool
) -> Page:
    """Get a page from an SQLAlchemy ORM query.

    :param q: The :class:`Query` to paginate.
    :param per_page: Number of rows per page.
    :param place: Keyset representing the place after which to start the page.
    :param backwards: If ``True``, reverse pagination direction.
    :returns: :class:`Page`
    """
    result_type = orm_result_type(q)
    keys = orm_query_keys(q)
    paging_query = prepare_paging(
        q=q,
        per_page=per_page,
        place=place,
        backwards=backwards,
        orm=True,
        dialect=q.session.get_bind().dialect,
    )
    rows = paging_query.query.all()
    page = orm_page_from_rows(
        paging_query, rows, keys, result_type, per_page, backwards, current_place=place
    )
    return page


def core_get_page(
    s: Union[Session, Connection],
    selectable: Select[_TP],
    per_page: int,
    place: Optional[Keyset],
    backwards: bool,
) -> Page[Row[_TP]]:
    """Get a page from an SQLAlchemy Core selectable.

    :param s: :class:`sqlalchemy.engine.Connection` or
        :class:`sqlalchemy.orm.session.Session` to use to execute the query.
    :param selectable: The source selectable.
    :param per_page: Number of rows per page.
    :param place: Keyset representing the place after which to start the page.
    :param backwards: If ``True``, reverse pagination direction.
    :returns: :class:`Page`
    """
    # In SQLAlchemy 1.3, we need the result schema for the *original* query in order
    # to properly trim off our extra_columns. As far as I can tell, this is the only
    # way to get it without copy-pasting chunks of the sqlalchemy internals.
    # LIMIT 0 to minimize database load (though the fact that a round trip to
    # the DB has to happen at all is regrettable).
    #
    # Thankfully this is obsolete in 1.4+
    result_type = core_result_type(selectable, s)
    sel = prepare_paging(
        q=selectable,
        per_page=per_page,
        place=place,
        backwards=backwards,
        orm=False,
        dialect=get_bind(q=selectable, s=s).dialect,
    )
    selected = s.execute(sel.select)
    keys = list(selected.keys())
    N = len(keys) - len(sel.extra_columns)
    keys = keys[:N]
    page = core_page_from_rows(
        sel,
        selected.fetchall(),
        keys,
        result_type,
        per_page,
        backwards,
        current_place=place,
    )
    return page


def core_page_from_rows(
    paging_select: _PagingSelect,
    rows: Sequence,
    keys: List[str],
    result_type,
    page_size: int,
    backwards: bool = False,
    current_place: Optional[Keyset] = None,
) -> Page[Row]:
    """Turn a raw page of results for an SQLAlchemy Core query (as obtained by
    :func:`.core_get_page`) into a :class:`.Page` for external consumers."""
    _, _, mapped_ocols, extra_columns = paging_select

    make_row = partial(
        core_coerce_row, extra_columns=extra_columns, result_type=result_type
    )
    out_rows = [make_row(row) for row in rows]
    key_rows = [tuple(col.get_from_row(row) for col in mapped_ocols) for row in rows]
    paging = Paging(out_rows, page_size, backwards, current_place, places=key_rows)
    page = Page(paging.rows, paging, keys=keys)
    return page


# Sadly the default values for after/before used to be False, not None, so we
# need to support either of these to avoid breaking API compatibility.
# Thus this awful type.
OptionalKeyset = Union[Keyset, Literal[False], None]


def process_args(
    after: OptionalKeyset = None,
    before: OptionalKeyset = None,
    page: Optional[Union[MarkerLike, str]] = None,
) -> Marker:
    if isinstance(page, str):
        page = unserialize_bookmark(page)

    if after is False:
        after = None

    if before is False:
        before = None

    if before is not None and after is not None:
        raise ValueError("after *OR* before")

    if (before is not None or after is not None) and page is not None:
        raise ValueError("specify either a page tuple, or before/after")

    if page:
        try:
            place, backwards = page
        except ValueError as e:
            raise InvalidPage("page is not a recognized string or marker tuple") from e
    elif after:
        place = after
        backwards = False
    elif before:
        place = before
        backwards = True
    else:
        backwards = False
        place = None

    if place is not None and not isinstance(place, tuple):
        raise ValueError("Keyset (after, before or page[0]) must be a tuple or None")

    return Marker(place, backwards)


def select_page(
    s: Union[Session, Connection],
    selectable: Select[_TP],
    per_page: int = PER_PAGE_DEFAULT,
    after: OptionalKeyset = None,
    before: OptionalKeyset = None,
    page: Optional[Union[MarkerLike, str]] = None,
) -> Page[Row[_TP]]:
    """Get a page of results from a SQLAlchemy Core (or new-style ORM) selectable.

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


def get_page(
    query: Query[_TP],
    per_page: int = PER_PAGE_DEFAULT,
    after: OptionalKeyset = None,
    before: OptionalKeyset = None,
    page: Optional[Union[MarkerLike, str]] = None,
) -> Page[Row[_TP]]:
    """Get a page of results for a legacy ORM query.

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


@dataclass
class OrmPageRequest(Generic[_TP]):
    """See ``get_page()`` documentation for parameter explanations."""
    query: Query[_TP]
    per_page: int = PER_PAGE_DEFAULT
    after: OptionalKeyset = None
    before: OptionalKeyset = None
    page: Optional[Union[MarkerLike, str]] = None


@dataclass
class PageRequest(Generic[_TP]):
    """See ``select_page()`` documentation for parameter explanations."""
    selectable: Select[_TP]
    per_page: int = PER_PAGE_DEFAULT
    after: OptionalKeyset = None
    before: OptionalKeyset = None
    page: Optional[Union[MarkerLike, str]] = None


def get_homogeneous_pages(requests: list[OrmPageRequest[_TP]]) -> list[Page[Row[_TP]]]:
    """Get multiple pages of results for homogeneous legacy ORM queries.

    This only involves a single round trip to the database. To do that, under the
    hood it generates a UNION ALL. That means each query must select exactly the
    same columns. They may have different filters or ordering, but must result in
    selecting the same columns with the same names.

    Note: This requires the underlying database to support ORDER BY and LIMIT
    statements in components of a compound select, which SQLite does not.

    Resulting pages are returned in the same order as the original page requests.
    """
    if not requests:
        return []

    ordering_infos = _get_ordering_infos(requests, orm=True)
    prepared_queries = [
        _orm_prepare_homogeneous_page(request, ordering_infos[i], i)
        for i, request in enumerate(requests)
    ]

    query = prepared_queries[0].paging_query.query
    query = query.union_all(
        *[p.paging_query.query for p in prepared_queries[1:]]
    ).order_by(text("_page_identifier"), text("_row_number"))

    results = query.all()

    # We need to make sure there's an entry for every page in case some return
    # empty.
    page_to_rows = {i: list() for i in range(len(requests))}
    for row in results:
        page_to_rows[row._page_identifier].append(row)

    pages = []
    for i in range(len(requests)):
        rows = page_to_rows[i]
        pages.append(prepared_queries[i].page_from_rows(rows))
    return pages


def select_homogeneous_pages(
    requests: list[PageRequest[_TP]], s: Union[Session, Connection]
) -> list[Page[Row[_TP]]]:
    """Get multiple pages of results for homogeneous legacy ORM queries.

    This only involves a single round trip to the database. To do that, under the
    hood it generates a UNION ALL. That means each query must select exactly the
    same columns. They may have different filters or ordering, but must result in
    selecting the same columns with the same names.

    Note: This requires the underlying database to support ORDER BY and LIMIT
    statements in components of a compound select, which SQLite does not.

    Resulting pages are returned in the same order as the original page requests.
    """
    if not requests:
        return []

    if len(requests) == 1:
        # Handling 1 request is annoying because of its effect on union_all,
        # so it's easier to just farm it out.
        request = requests[0]
        return [
            select_page(
                s,
                request.selectable,
                per_page=request.per_page,
                after=request.after,
                before=request.before,
                page=request.page
            )
        ]

    # Because UNION ALL requires identical SELECT statements, but we allow different
    # order_bys which could result in different extra columns for order keys, we need
    # to first find the superset of extra columns and then add those to ever single
    # selectable.

    ordering_infos = _get_ordering_infos(requests, orm=False)

    prepared_queries = [
        _core_prepare_homogeneous_page(request, s, ordering_infos[i], i)
        for i, request in enumerate(requests)
    ]

    selectable = union_all(
        *[p.paging_query.select for p in prepared_queries]
    ).order_by(text("_page_identifier"), text("_row_number"))

    columns = prepared_queries[0].paging_query.select._raw_columns
    selectable = select(*columns).from_statement(selectable)

    compiled = selectable.compile(compile_kwargs={"literal_binds": True})
    print(f"Select from statement: {compiled}")
    selected = s.execute(selectable)

    results = selected.fetchall()

    # We need to make sure there's an entry for every page in case some return
    # empty.
    page_to_rows = {i: list() for i in range(len(requests))}
    for row in results:
        page_to_rows[row._page_identifier].append(row)

    pages = []

    keys = list(selected.keys())
    N = len(keys) - len(prepared_queries[0].paging_query.extra_columns)
    keys = keys[:N]

    for i in range(len(requests)):
        rows = page_to_rows[i]
        pages.append(prepared_queries[i].page_from_rows(rows, keys))
    return pages


@dataclass
class _OrderingInfo:
    order_cols: list[OC] = field(default_factory=list)
    mapped_ocols: list[MappedOrderColumn] = field(default_factory=list)
    extra_columns: list[ColumnElement] = field(default_factory=list)


def _get_ordering_infos(requests, orm) -> list[_OrderingInfo]:
    infos = []
    extra_column_mappers: dict[str, MappedOrderColumn] = {}

    for request in requests:
        info = _OrderingInfo()
        infos.append(info)
        if orm:
            if not isinstance(request, OrmPageRequest):
                raise ValueError("If orm=True then requests must be OrmPageRequests")
            selectable = orm_to_selectable(request.query)
            column_descriptions = request.query.column_descriptions
        else:
            if isinstance(request, OrmPageRequest):
                raise ValueError("If orm=False then q cannot be a OrmPageRequest")
            selectable = request.selectable
            try:
                column_descriptions = selectable.column_descriptions
            except Exception:
                column_descriptions = selectable._raw_columns  # type: ignore

        order_cols = parse_ob_clause(selectable)
        place, backwards = process_args(request.after, request.before, request.page)
        if backwards:
            order_cols = [c.reversed for c in order_cols]
        info.order_cols = order_cols

        mapped_ocols = [find_order_key(ocol, column_descriptions) for ocol in order_cols]
        for i, col in enumerate(list(mapped_ocols)):
            if col.extra_column is None:
                continue
            name = OC(col.extra_column).quoted_full_name
            if name in extra_column_mappers:
                mapped_ocols[i] = extra_column_mappers[name]
                # Since we cache these mappers across different selects, we need
                # to fix up any ordering here.
                if mapped_ocols[i].oc.is_ascending != order_cols[i].is_ascending:
                    mapped_ocols[i] = mapped_ocols[i].reversed
            else:
                extra_column_mappers[name] = col

        info.mapped_ocols = mapped_ocols

    extra_columns = [col.extra_column for col in extra_column_mappers.values()]
    print(f"Extra columns: {extra_columns}")
    for i, info in enumerate(infos):
        info.extra_columns = list(extra_columns) + [
            literal(i).label("_page_identifier"),
            func.ROW_NUMBER().over(
                order_by=[c.uo for c in info.order_cols]
            ).label("_row_number"),
        ]

    return infos


@dataclass
class _PreparedQuery:
    paging_query: Union[_PagingQuery, _PagingSelect]
    page_from_rows: Callable[..., Page[Row[_TP]]]


def _core_prepare_homogeneous_page(
    request: PageRequest[_TP],
    s: Union[Session, Connection],
    info: _OrderingInfo,
    page_identifier: int
) -> _PreparedQuery:
    place, backwards = process_args(request.after, request.before, request.page)

    selectable = request.selectable
    result_type = core_result_type(selectable, s)

    clauses = [col.ob_clause for col in info.mapped_ocols]
    selectable = selectable.order_by(None).order_by(*clauses)

    selectable = selectable.add_columns(*info.extra_columns)
    selectable = _apply_where_and_limit(
        selectable,
        selectable,
        request.per_page,
        place,
        get_bind(q=selectable, s=s).dialect,
        info.order_cols,
        orm=False
    )
    sel = _PagingSelect(selectable, info.order_cols, info.mapped_ocols, info.extra_columns)

    def page_from_rows(rows, keys):
        page = core_page_from_rows(
            sel,
            rows,
            keys,
            result_type,
            request.per_page,
            backwards,
            current_place=place,
        )
        return page

    return _PreparedQuery(paging_query=sel, page_from_rows=page_from_rows)


def _orm_prepare_homogeneous_page(
    request: OrmPageRequest[_TP], info: _OrderingInfo, page_identifier: int
) -> _PreparedQuery:
    place, backwards = process_args(request.after, request.before, request.page)

    query = request.query
    result_type = orm_result_type(query)
    keys = orm_query_keys(query)

    clauses = [col.ob_clause for col in info.mapped_ocols]
    query = query.order_by(None).order_by(*clauses)

    if hasattr(query, "add_columns"):  # ORM or SQLAlchemy 1.4+
        print(f"Adding extra columns: {info.extra_columns}")
        query = query.add_columns(*info.extra_columns)
    else:
        for col in info.extra_columns:  # SQLAlchemy Core <1.4
            query = query.column(col)  # type: ignore

    query = _apply_where_and_limit(
        query,
        orm_to_selectable(query),
        request.per_page,
        place,
        query.session.get_bind().dialect,
        info.order_cols,
        orm=True
    )
    paging_query = _PagingQuery(query, info.order_cols, info.mapped_ocols, info.extra_columns)

    def page_from_rows(rows):
        return orm_page_from_rows(
            paging_query, rows, keys, result_type, request.per_page, backwards, current_place=place
        )

    return _PreparedQuery(paging_query=paging_query, page_from_rows=page_from_rows)
