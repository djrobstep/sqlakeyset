"""The OC class and supporting functions to manipulate ordering columns."""
from warnings import warn
from copy import copy

import sqlalchemy
from sqlalchemy import asc, column
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Bundle, Mapper, class_mapper
from sqlalchemy.sql.expression import Label, ClauseList
from sqlalchemy.sql.elements import _label_reference
from sqlalchemy.sql.operators import asc_op, desc_op, nullsfirst_op, nullslast_op

_LABELLED = (Label, _label_reference)
_ORDER_MODIFIERS = (asc_op, desc_op, nullsfirst_op, nullslast_op)
_UNSUPPORTED_ORDER_MODIFIERS = (nullsfirst_op, nullslast_op)
_WRAPPING_DEPTH = 1000
_WRAPPING_OVERFLOW = ("Maximum element wrapping depth reached; there's "
                      "probably a circularity in sqlalchemy that "
                      "sqlakeyset doesn't know how to handle.")


def parse_clause(clause):
    """Parse an ORDER BY clause into a list of :class:`OC` instances."""
    def _flatten(cl):
        if isinstance(cl, ClauseList):
            for subclause in cl.clauses:
                # TODO: could use "yield from" here if we require python>=3.3
                for x in _flatten(subclause):
                    yield x
        else:
            yield cl
    return [OC(c) for c in _flatten(clause)]


def _warn_if_nullable(x):
    try:
        if x.nullable or x.property.columns[0].nullable:
            warn(f"Ordering by nullable column {x} can cause rows to be "
                 "incorrectly omitted from the results. "
                 "See the sqlakeyset README for more details.")
    except (AttributeError, IndexError, KeyError):
        pass

class OC:
    """Wrapper class for ordering columns; i.e. ColumnElements appearing in
    the ORDER BY clause of a query we are paging."""
    def __init__(self, x):
        if isinstance(x, str):
            x = column(x)
        if _get_order_direction(x) is None:
            x = asc(x)
        self.uo = x
        _warn_if_nullable(self.comparable_value)
        self.full_name = str(self.element)
        try:
            table_name, name = self.full_name.split('.', 1)
        except ValueError:
            table_name = None
            name = self.full_name

        self.table_name = table_name
        self.name = name

    @property
    def quoted_full_name(self):
        return str(self).split()[0]

    @property
    def element(self):
        """The ordering column/SQL expression with ordering modifier removed."""
        return _remove_order_direction(self.uo)

    @property
    def comparable_value(self):
        """The ordering column/SQL expression in a form that is suitable for
        incorporating in a ROW(...)>ROW(...) comparision; i.e. with ordering
        modifiers and labels removed."""
        return strip_labels(self.element)

    @property
    def is_ascending(self):
        """Returns ``True`` if this column is ascending, ``False`` if
        descending."""
        d = _get_order_direction(self.uo)
        if d is None:
            raise ValueError  # pragma: no cover
        return d == asc_op

    @property
    def reversed(self):
        """An :class:`OC` representing the same column ordering, but reversed."""
        new_uo = _reverse_order_direction(self.uo)
        if new_uo is None:
            raise ValueError
        return OC(new_uo)

    def __str__(self):
        return str(self.uo)

    def __repr__(self):
        return '<OC: {}>'.format(str(self))

def strip_labels(el):
    """Remove labels from a ColumnElement."""
    while isinstance(el, _LABELLED):
        try:
            el = el.element
        except AttributeError:
            raise ValueError
    return el


def _get_order_direction(x):
    """
    Given a ColumnElement, find and return its ordering direction
    (ASC or DESC) if it has one.

    :param x: a :class:`sqlalchemy.sql.expression.ColumnElement`
    :return: `asc_op`, `desc_op` or `None`
    """
    for _ in range(_WRAPPING_DEPTH):
        mod = getattr(x, 'modifier', None)
        if mod in (asc_op, desc_op):
            return mod

        el = getattr(x, 'element', None)
        if el is None:
            return None
        x = el
    raise Exception(_WRAPPING_OVERFLOW)


def _reverse_order_direction(ce):
    """
    Given a ColumnElement, return a copy with its ordering direction
    (ASC or DESC) reversed (if it has one).

    :param ce: a :class:`sqlalchemy.sql.expression.ColumnElement`
    """
    x = copied = ce._clone()
    for _ in range(_WRAPPING_DEPTH):
        mod = getattr(x, 'modifier', None)
        if mod in (asc_op, desc_op):
            if mod == asc_op:
                x.modifier = desc_op
            else:
                x.modifier = asc_op
            return copied
        else:
            if not hasattr(x, 'element'):
                return copied
            # Since we're going to change something inside x.element, we
            # need to clone another level deeper.
            x._copy_internals()
            x = x.element
    raise Exception(_WRAPPING_OVERFLOW)


def _remove_order_direction(ce):
    """
    Given a ColumnElement, return a copy with its ordering modifiers
    (ASC/DESC, NULLS FIRST/LAST) removed (if it has any).

    :param ce: a :class:`sqlalchemy.sql.expression.ColumnElement`
    """
    x = copied = ce._clone()
    parent = None
    for _ in range(_WRAPPING_DEPTH):
        mod = getattr(x, 'modifier', None)
        if mod in _UNSUPPORTED_ORDER_MODIFIERS:
            warn("One of your order columns had a NULLS FIRST or NULLS LAST "
                 "modifier; but sqlakeyset does not support order columns "
                 "with nulls. YOUR RESULTS WILL BE WRONG. See the "
                 "Limitations section of the sqlakeyset README for more "
                 "information.")
        if mod in _ORDER_MODIFIERS:
            x._copy_internals()
            if parent is None:
                # The modifier was at the top level; so just take the child.
                copied = x = x.element
            else:
                # Remove this link from the wrapping element chain and return
                # the top-level expression.
                parent.element = x = x.element
        else:
            if not hasattr(x, 'element'):
                return copied
            parent = x
            # Since we might change something inside x.element, we
            # need to clone another level deeper.
            x._copy_internals()
            x = x.element
    raise Exception(_WRAPPING_OVERFLOW)


class MappedOrderColumn:
    """An ordering column in the context of a particular query/select.

    This wraps an OC with one extra piece of information: how to retrieve the
    value of the ordering key from a result row. For some queries, this
    requires adding extra entities to the query; in this case,
    ``extra_entity`` will be set."""

    extra_entity = None
    def __init__(self, oc):
        self.oc = oc

    @property
    def ob_clause(self):
        return self.oc.uo

    @property
    def reversed(self):
        c = copy(self)
        c.oc = c.oc.reversed
        return c


class DerivedKey(MappedOrderColumn):
    """An ordering key that can be derived from the original query results."""
    def __init__(self, oc, getter):
        super().__init__(oc)
        self.getter = getter

    def get_from_row(self, internal_row):
        return self.getter(internal_row)


class AppendedKey(MappedOrderColumn):
    """An ordering key that requires an additional column to be added to the
    original query."""
    _counter = 0
    def __init__(self, oc, name=None):
        super().__init__(oc)
        if not name:
            AppendedKey._counter += 1
            name = "_sqlakeyset_oc_{}".format(AppendedKey._counter)
        self.name = name
        self.extra_entity = self.oc.comparable_value.label(self.name)

    def get_from_row(self, internal_row):
        return getattr(internal_row, self.name)

    @property
    def ob_clause(self):
        return self.extra_entity

def ColumnDerivedKey(colname, oc, getter):
    """Convenience wrapper - an ordering key that can be derived from a single
    column of the original query results."""
    return DerivedKey(oc, lambda row: getter(getattr(row, colname)))


def find_order_key(ocol, column_descriptions):
    """Return a :class:`MappedOrderColumn` describing how to populate the
    ordering column `ocol` from a query returning columns described by
    `column_descriptions`.

    :param ocol: The :class:`OC` to look up.
    :param column_descriptions: The list of columns from which to attempt to
        derive the value of ``ocol``.
    :returns: A :class:`MappedOrderColumn` wrapping ``ocol``.
"""
    for desc in column_descriptions:
        name = desc['name']
        entity = desc['entity']
        expr = desc['expr']

        if isinstance(expr, Bundle):
            for prop, col in expr.columns.items():
                if strip_labels(col) == ocol.comparable_value:
                    return ColumnDerivedKey(name, ocol,
                                            lambda c: getattr(c, prop))

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
                return ColumnDerivedKey(name, ocol,
                                        lambda c: getattr(c, prop.key))
            except sqlalchemy.orm.exc.UnmappedColumnError:
                pass

        # is an attribute
        if hasattr(expr, 'info'):
            mapper = expr.parent
            tname = mapper.local_table.description
            if ocol.table_name == tname and ocol.name == expr.name:
                return ColumnDerivedKey(name, ocol, lambda c: c)

        # is an attribute with label
        try:
            if ocol.quoted_full_name == OC(expr).full_name:
                return ColumnDerivedKey(name, ocol, lambda c: c)
        except ArgumentError:
            pass

    # Couldn't find an existing column in the query from which we can
    # determine this ordering column; so we need to add one.
    return AppendedKey(ocol)

