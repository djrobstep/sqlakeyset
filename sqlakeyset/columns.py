"""The OC class and supporting functions to manipulate ordering columns."""
from __future__ import (absolute_import, division, print_function, unicode_literals)

import sys

from sqlalchemy import asc, column
from sqlalchemy.sql.expression import UnaryExpression, Label
from sqlalchemy.sql.elements import _label_reference
from sqlalchemy.sql.operators import asc_op, desc_op, nullsfirst_op, nullslast_op

_LABELLED = (Label, _label_reference)
_ORDER_MODIFIERS = (asc_op, desc_op, nullsfirst_op, nullslast_op)
_WRAPPING_DEPTH = 1000
_WRAPPING_OVERFLOW = ("Maximum element wrapping depth reached; there's "
                      "probably a circularity in sqlalchemy that "
                      "sqlakeyset doesn't know how to handle.")

PY2 = sys.version_info.major <= 2

if not PY2:
    unicode = str


def parse_clause(clause):
    return [OC(c) for c in clause]
   

class OC(object):
    """Wrapper class for ordering columns; i.e. ColumnElements appearing in
    the ORDER BY clause of a query we are paging."""
    def __init__(self, x):
        if isinstance(x, unicode):
            x = column(x)
        if _get_order_direction(x) is None:
            x = asc(x)
        self.uo = x
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
        """Get a copy of the wrapped column with ordering modifier removed."""
        return _remove_order_direction(self.uo)

    @property
    def comparable_value(self):
        """Get a copy of the wrapped column that is suitable for incorporating
        in a ROW(...)>ROW(...) comparision; i.e. with ordering modifiers and
        labels removed."""
        el = self.element
        while isinstance(el, _LABELLED):
            try:
                el = el.element
            except AttributeError:
                raise ValueError
        return el

    @property
    def is_ascending(self):
        d = _get_order_direction(self.uo)
        if d is None:
            raise ValueError  # pragma: no cover
        return d == asc_op

    @property
    def reversed(self):
        """Get an OC representing the same column ordering, but reversed."""
        # TODO: swapping asc/desc does NOT exactly reverse the order when nulls
        # are present. We should use NULLS FIRST/NULLS LAST appropriately, at
        # least when the SQL dialect supports them.
        new_uo = _reverse_order_direction(self.uo)
        if new_uo is None:
            raise ValueError
        return OC(new_uo)

    def __str__(self):
        return str(self.uo)

    def __repr__(self):
        return '<OC: {}>'.format(str(self))


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

    Does NOT yet handle NULLS FIRST/LAST correctly.

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
