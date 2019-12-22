"""Paging data structures and bookmark handling."""
from __future__ import unicode_literals

import csv

from .serial import Serial

SERIALIZER_SETTINGS = dict(
    lineterminator=str(''),
    delimiter=str('~'),
    doublequote=False,
    escapechar=str('\\'),
    quoting=csv.QUOTE_NONE)

s = Serial(**SERIALIZER_SETTINGS)


def serialize_bookmark(x):
    """Serialize a place marker to a bookmark string."""
    x, backwards = x
    ss = s.serialize_values(x)
    direction = '<' if backwards else '>'
    return direction + ss


def unserialize_bookmark(x):
    """Deserialize a bookmark string to a place marker."""
    if not x:
        return None, False

    direction = x[0]

    if direction not in ('>', '<'):
        raise ValueError

    backwards = direction == '<'
    cells = s.unserialize_values(x[1:])
    return cells, backwards


class Page(list):
    """A :class:`list` of result rows with access to paging information and
    some convenience methods."""
    def scalar(self):
        """Assuming paging was called with ``per_page=1`` and a single-column
        query, return the single value."""
        return self.one()[0]

    def one(self):
        """Assuming paging was called with ``per_page=1``, return the single
        row on this page."""
        c = len(self)

        if c < 1:
            raise RuntimeError('tried to select one but zero rows returned')
        elif c > 1:
            raise RuntimeError('too many rows returned')
        else:
            return self[0]


class Paging:
    """Object with paging information. Most properties return a page marker.
    Prefix these properties with ``bookmark_`` to get the serialized version of
    that page marker."""

    def __init__(
            self,
            rows,
            per_page,
            ocols,
            backwards,
            current_marker,
            get_marker=None,
            markers=None):

        if get_marker:
            marker = lambda i: get_marker(rows[i], ocols)
        else:
            if rows and not markers:
                raise ValueError
            marker = markers.__getitem__

        self.original_rows = rows

        self.per_page = per_page
        self.backwards = backwards

        excess = rows[per_page:]
        self.marker_0 = current_marker

        if rows:
            self.marker_1 = marker(0)
            self.marker_n = marker(min(per_page, len(rows)) - 1)
        else:
            self.marker_1 = None
            self.marker_n = None

        if excess:
            self.marker_nplus1 = marker(per_page)
        else:
            self.marker_nplus1 = None

        four = [self.marker_0, self.marker_1, self.marker_n, self.marker_nplus1]

        rows = rows[:per_page]
        self.rows = rows

        if backwards:
            self.rows.reverse()
            four.reverse()

        self.before, self.first, self.last, self.beyond = four

    @property
    def has_next(self):
        """Boolean flagging whether there are more rows after this page (in the
        original query order)."""
        return bool(self.beyond)

    @property
    def has_previous(self):
        """Boolean flagging whether there are more rows before this page (in the
        original query order)."""
        return bool(self.before)

    @property
    def next(self):
        """Marker for the next page (in the original query order)."""
        return (self.last or self.before), False

    @property
    def previous(self):
        """Marker for the previous page (in the original query order)."""
        return (self.first or self.beyond), True

    @property
    def current_forwards(self):
        """Marker for the current page in forwards direction."""
        return self.before, False

    @property
    def current_backwards(self):
        """Marker for the current page in backwards direction."""
        return self.beyond, True

    @property
    def current(self):
        """Marker for the current page in the current direction."""
        if self.backwards:
            return self.current_backwards
        else:
            return self.current_forwards

    @property
    def current_opposite(self):
        """Marker for the current page in the opposite of the current
        direction."""
        if self.backwards:
            return self.current_forwards
        else:
            return self.current_backwards

    @property
    def further(self):
        """Marker for the following page in the paging direction (as modified
        by ``backwards``)."""
        if self.backwards:
            return self.previous
        else:
            return self.next

    @property
    def has_further(self):
        """Boolean flagging whether there are more rows before this page in the
        paging direction (as modified by ``backwards``)."""
        if self.backwards:
            return self.has_previous
        else:
            return self.has_next

    @property
    def is_full(self):
        """Boolean flagging whether this page contains as many rows as were
        requested in ``per_page``."""
        return len(self.rows) == self.per_page

    def __getattr__(self, name):
        PREFIX = 'bookmark_'
        if name.startswith(PREFIX):
            _, attname = name.split(PREFIX, 1)
            x = getattr(self, attname)
            return serialize_bookmark(x)

        raise AttributeError
