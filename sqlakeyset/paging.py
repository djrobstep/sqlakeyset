"""Main paging interface and implementation."""
from __future__ import unicode_literals

import sys
from functools import partial

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.orm import class_mapper, Mapper
from sqlalchemy.util import lightweight_named_tuple
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm.exc import UnmappedColumnError

from .columns import parse_clause, OC
from .results import Page, Paging, unserialize_bookmark

PER_PAGE_DEFAULT = 10

PY2 = sys.version_info[0] == 2

if not PY2:
    unicode = str


def where_condition_for_page(ordering_columns, place):
    row, place_row = paging_condition(ordering_columns, place)
    if len(row) == 1:
        condition = row[0] > place_row[0]
    else:
        condition = func.row(*row) > func.row(*place_row)
    return condition


def orm_clean_row(row, key_entities, result_type):
    if key_entities:
        n = len(row) - len(key_entities)
        return result_type(row[:n]), row[n:]
    else:
        return row, ()


def orm_page_from_rows(
        rows,
        page_size,
        ocols,
        column_descriptions,
        key_entities,
        result_type,
        backwards=False,
        current_marker=None):
    # orm_get_page might have added some extra columns to the query in order
    # to get the keys for the bookmark. Here, we extract those keys into a
    # dict, and then trim the rows back to their original format.
    rows_and_keys = [orm_clean_row(row, key_entities, result_type)
                     for row in rows]
    rows = [r for r, _ in rows_and_keys]
    row_keys=dict(rows_and_keys)

    get_marker = partial(
        orm_placemarker_from_row,
        column_descriptions=column_descriptions,
        key_entities=key_entities,
        row_keys=row_keys,
    )

    paging = Paging(rows, page_size, ocols, backwards, current_marker, get_marker)

    page = Page(paging.rows)
    page.paging = paging
    return page


def core_page_from_rows(
        rows,
        page_size,
        ocols,
        backwards=False,
        current_marker=None,
        keys=None):
    paging = Paging(rows, page_size, ocols, backwards, current_marker, core_placemarker_from_row)

    page = Page(paging.rows)
    page.paging = paging
    page._keys = keys
    return page


def value_from_thing(thing, desc, ocol):
    entity = desc['entity']
    expr = desc['expr']

    try:
        # We need to coerce this to a bool now to catch TypeErrors in certain
        # cases where (entity == expr) is a SQLAlchemy expression with no
        # truth value.
        is_a_table = bool(entity == expr)
    except (sqlalchemy.exc.ArgumentError, TypeError):
        is_a_table = False

    if isinstance(expr, Mapper) and expr.class_ == entity:
        # Is a table mapper. Just treat as a table.
        is_a_table = True

    if is_a_table:  # is a table
        mapper = class_mapper(desc['type'])
        try:
            prop = mapper.get_property_by_column(ocol.element)
            return getattr(thing, prop.key)
        except UnmappedColumnError:
            raise ValueError

    # is an attribute
    if hasattr(expr, 'info'):
        mapper = expr.parent
        tname = mapper.local_table.description

        if ocol.table_name == tname and ocol.name == expr.name:
            return thing
        else:
            raise ValueError

    # is an attribute with label
    if ocol.quoted_full_name == OC(expr).full_name:
        return thing
    else:
        raise ValueError


def orm_placemarker_from_row(row, ocols, column_descriptions,
                             key_entities, row_keys):
    cant_find = "can't find value for column {} in the results returned"
    one_entity = len(column_descriptions) == 1
    def get_value(ocol):
        if ocol in key_entities:
            # We added this ocol to the query explicitly; so we can just go 
            # and get it.
            index, _ = key_entities[ocol]
            return row_keys[row][index]
        for thing, desc in zip([row] if one_entity else row,
                               column_descriptions):
            try:
                return value_from_thing(thing, desc, ocol)
            except ValueError:
                continue
        raise ValueError(cant_find.format(ocol.full_name))

    return tuple(get_value(x) for x in ocols)


def core_placemarker_from_row(row, ocols):
    def get_value(ocol):
        return row[ocol.name]

    return tuple(get_value(x) for x in ocols)


def orm_key_entity(ocol, column_descriptions):
    """Determine whether the value of `ocol` can be derived from a result row
    described by `column_descriptions`. If so, return None. If not, return an
    extra column expression containing the value of `ocol`."""
    for desc in column_descriptions:
        entity = desc['entity']
        expr = desc['expr']

        try:
            is_a_table = bool(entity == expr)
        except (sqlalchemy.exc.ArgumentError, TypeError):
            is_a_table = False

        if isinstance(expr, Mapper) and expr.class_ == entity:
            is_a_table = True

        if is_a_table:  # is a table
            mapper = class_mapper(desc['type'])
            try:
                prop = mapper.get_property_by_column(ocol.element)
                return None
            except UnmappedColumnError:
                pass

        # is an attribute
        if hasattr(expr, 'info'):
            mapper = expr.parent
            tname = mapper.local_table.description
            if ocol.table_name == tname and ocol.name == expr.name:
                return None

        # is an attribute with label
        try:
            if ocol.quoted_full_name == OC(expr).full_name:
                return None
        except ArgumentError:
            pass

    # Couldn't find an existing column in the query from which we can
    # determine this ordering column; so we need to add one.
    return ocol.element


def orm_key_entities(ocols, column_descriptions, starting_index=0):
    es = ((starting_index + i, c, orm_key_entity(c, column_descriptions))
          for i, c in enumerate(ocols))
    return {c: (i,e) for i, c, e in es if e is not None}


def orm_result_type(query):
    labels = [e._label_name for e in query._entities]

    if len(labels) > 1:
        return lightweight_named_tuple("result", labels)
    else:
        return lambda x: x[0]


def orm_get_page(q, per_page, place, backwards):
    ob_clause = q.selectable._order_by_clause

    order_cols = parse_clause(ob_clause)

    if backwards:
        order_cols = [c.reversed for c in order_cols]

    clauses = [c.uo for c in order_cols]
    q = q.order_by(False).order_by(*clauses)
    column_descriptions = q.column_descriptions

    # We might have to add some entities to the query in order to get our
    # bookmark; so first save the result type of the original query so we can
    # restore it later.
    # TODO: is there a cleaner way to do this? Perhaps similarly to how sa's
    # joinedload, etc, works?
    result_type = orm_result_type(q)
    # Then work out which entities we need to add, and add them.
    key_entities = orm_key_entities(order_cols, q.column_descriptions)
    existing_entities = (e.expr for e in q._entities)
    q = q.with_entities(
        *existing_entities,
        *(key_ent for (_, key_ent) in key_entities.values())
    )

    if place:
        condition = where_condition_for_page(order_cols, place)
        # For aggregate queries, paging condition is applied *after*
        # aggregation. In SQL this means we need to use HAVING instead of
        # WHERE.
        if q._group_by:
            q = q.having(condition)
        else:
            q = q.filter(condition)

    q = q.limit(per_page + 1)

    rows = q.all()

    page = orm_page_from_rows(
        rows,
        per_page,
        order_cols,
        column_descriptions,
        key_entities,
        result_type,
        backwards,
        current_marker=place)

    return page


def core_get_page(s, selectable, per_page, place, backwards):
    order_cols = parse_clause(selectable._order_by_clause)

    if backwards:
        order_cols = [c.reversed for c in order_cols]
    clauses = [x.uo for x in order_cols]

    selectable = selectable.order_by(None).order_by(*clauses)

    if place:
        where = where_condition_for_page(order_cols, place)
        selectable = selectable.where(where)

    selectable = selectable.limit(per_page + 1)

    selected = s.execute(selectable)
    rowkeys = selected.keys()
    rows = selected.fetchall()

    page = core_page_from_rows(
        rows,
        per_page,
        order_cols,
        backwards,
        current_marker=place,
        keys=rowkeys)

    return page


def paging_condition(ordering_columns, place):
    if len(ordering_columns) != len(place):
        raise ValueError('bad paging value')

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
    if isinstance(page, unicode):
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
    place, backwards = process_args(after, before, page)

    return core_get_page(
        s,
        selectable,
        per_page,
        place,
        backwards)


def get_page(
        q,
        per_page=PER_PAGE_DEFAULT,
        after=False,
        before=False,
        page=False):
    place, backwards = process_args(after, before, page)

    return orm_get_page(
        q,
        per_page,
        place,
        backwards)
