"""
Main paging interface.

The modules in this directory are a heavily modified version of the "sqlakeyset" library, see:
1. https://github.com/djrobstep/sqlakeyset

We started by making the library compatible with `asyncio` and 2.0 SQLAlchemy style, and ended up only keeping the
parts we need.
"""
from typing import Any, Optional

from sqlalchemy import tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from sqlakeyset.columns import OC, find_order_key, parse_ob_clause
from sqlakeyset.results import Page, Paging
from sqlakeyset.serial import InvalidPage


def get_db() -> AsyncSession:
    # TODO: Integration is required here, to get the DB session.
    raise NotImplementedError


def where_condition_for_page(ordering_columns: list[OC], place: tuple[Any]):
    """
    Construct the SQL condition required to restrict a selectable to the desired page.

    :param ordering_columns: The query's ordering columns
    :param place: The starting position for the page
    :returns: An SQLAlchemy expression suitable for use in `.filter` or `.having`.

    Raises:
        InvalidPage: If `place` does not correspond to the given OCs.
    """
    db = get_db()

    if len(ordering_columns) != len(place):
        raise InvalidPage("Page marker has different column count to query's order clause")

    dialect = db.bind.dialect
    zipped = zip(ordering_columns, place)
    swapped = [c.pair_for_comparison(value, dialect) for c, value in zipped]
    row, place_row = zip(*swapped)

    if len(row) == 1:
        condition = row[0] > place_row[0]
    else:
        condition = tuple_(*row) > tuple_(*place_row)
    return condition


async def get_page(selectable, per_page: int, place: Optional[tuple[Any]], backwards: bool) -> Page:
    """
    Get a page from an SQLAlchemy Core selectable.

    Args:
        selectable: The source selectable.
        per_page: Number of rows per page.
        place: Keyset representing the place after which to start the page.
        backwards: If ``True``, reverse pagination direction.

    Returns:
        The result page.
    """
    db = get_db()

    # Build a list of ordering columns (ocols) in the form of `MappedOrderColumn` objects.
    order_cols = parse_ob_clause(selectable, backwards)
    mapped_ocols = [find_order_key(ocol, selectable.column_descriptions) for ocol in order_cols]

    # Update the selectable with the new order_by clauses.
    new_order_by_clauses = [col.ob_clause for col in mapped_ocols]
    selectable = selectable.order_by(None).order_by(*new_order_by_clauses)

    # Add the extra columns required for the ordering.
    extra_columns = [col.extra_column for col in mapped_ocols if col.extra_column is not None]
    selectable = selectable.add_columns(*extra_columns)

    if place:
        # Prepare the condition for selecting a specific page.
        condition = where_condition_for_page(order_cols, place)

        # If there is at least one GROUP BY clause, we have an aggregate query.
        # In this case, the paging condition is applied AFTER aggregation. To do so, we must use HAVING and not FILTER.
        if selectable._group_by_clauses:
            selectable = selectable.having(condition)
        else:
            selectable = selectable.where(condition)

    # Limit the amount of results in the page. The 1 extra is to check if there's a further page.
    selectable = selectable.limit(per_page + 1)

    # Run the selectable and get back the query rows.
    # NOTE: Do not use `.scalars` here, as it might lead to some rows being omitted by the ORM.
    selected = await db.execute(selectable)
    row_keys = list(selected.keys())
    rows = selected.all()

    # Finally, construct the `Page` object.
    # Trim off the extra columns and return as a correct-as-possible sqlalchemy Row.
    out_rows = [row[: -len(extra_columns) or None] for row in rows]
    key_rows = [tuple(col.get_from_row(row) for col in mapped_ocols) for row in rows]
    paging = Paging(out_rows, per_page, order_cols, backwards, place, markers=key_rows)
    return Page(paging.rows, paging, keys=row_keys[: -len(extra_columns) or None])
