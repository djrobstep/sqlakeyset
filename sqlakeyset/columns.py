from __future__ import (absolute_import, division, print_function, unicode_literals)

import sys
from copy import copy

from sqlalchemy import asc, column
from sqlalchemy.sql.expression import UnaryExpression
from sqlalchemy.sql.operators import asc_op, desc_op

PY2 = sys.version_info.major <= 2

if not PY2:
    unicode = str


def parse_clause(clause):
    return [OC(c) for c in clause]


class OC(object):
    def __init__(self, x):
        if isinstance(x, unicode):
            x = column(x)
        if not isinstance(x, UnaryExpression):
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
        x = self.uo
        while isinstance(x, UnaryExpression):
            if x.modifier in (asc_op, desc_op):
                return x.modifier == asc_op
            else:
                x = x.element
        raise ValueError  # pragma: no cover

    @property
    def reversed(self):
        x = copied = copy(self.uo)

        while isinstance(x, UnaryExpression):
            if x.modifier in (asc_op, desc_op):
                if x.modifier == asc_op:
                    x.modifier = desc_op
                else:
                    x.modifier = asc_op
                return OC(copied)
            else:
                x = x.element
        raise ValueError  # pragma: no cover

    def __str__(self):
        return str(self.uo)

    def __repr__(self):
        return '<OC: {}>'.format(str(self))
