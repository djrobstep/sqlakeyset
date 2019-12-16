from __future__ import (absolute_import, division, print_function, unicode_literals)

import sys

from sqlalchemy import asc, column
from sqlalchemy.sql.expression import UnaryExpression, Label
from sqlalchemy.sql.elements import _label_reference
from sqlalchemy.sql.operators import asc_op, desc_op

_COL_WRAPPERS = (UnaryExpression, Label, _label_reference)

PY2 = sys.version_info.major <= 2

if not PY2:
    unicode = str


def parse_clause(clause):
    return [OC(c) for c in clause]


def _get_order_direction(x):
    """
    Given a ColumnElement, find and return its ordering direction
    (ASC or DESC) if it has one.

    :param x: a :class:`sqlalchemy.sql.expression.ColumnElement`
    :return: `asc_op`, `desc_op` or `None`
    """
    try:
        for _ in range(1000):
            try:
                if x.modifier in (asc_op, desc_op):
                    return x.modifier == asc_op
            except AttributeError:
                pass
            x = x.element
        raise Exception("Insane element wrapping depth reached; there's "
                        "probably a sqlalchemy recursion here that "
                        "sqlakeyset doesn't know how to handle.")
    except AttributeError:
        return None


class OC(object):
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
        x = self.uo
        while isinstance(x, UnaryExpression):
            x = x.element
        return x

    @property
    def is_ascending(self):
        d = _get_order_direction(self.uo)
        if d is None:
            raise ValueError  # pragma: no cover
        return d

    @property
    def reversed(self):
        # It seems this "clone" is only one level deep; so we need to call
        # _copy_internals for each level we descend.
        x = copied = self.uo._clone()

        while isinstance(x, _COL_WRAPPERS):
            if getattr(x, 'modifier', None) in (asc_op, desc_op):
                if x.modifier == asc_op:
                    x.modifier = desc_op
                else:
                    x.modifier = asc_op
                return OC(copied)
            else:
                # Since we're going to change something inside x.element, we
                # need to clone another level deeper.
                x._copy_internals()
                x = x.element
        raise ValueError  # pragma: no cover

    def __str__(self):
        return str(self.uo)

    def __repr__(self):
        return '<OC: {}>'.format(str(self))
