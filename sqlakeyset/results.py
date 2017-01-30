from __future__ import unicode_literals

from .serial import Serial

import csv

SERIALIZER_SETTINGS = dict(
    lineterminator=str(''),
    delimiter=str('~'),
    doublequote=False,
    escapechar=str('\\'),
    quoting=csv.QUOTE_NONE)

s = Serial(**SERIALIZER_SETTINGS)


def serialize_bookmark(x):
    x, backwards = x
    ss = s.serialize_values(x)
    direction = '<' if backwards else '>'
    return direction + ss


def unserialize_bookmark(x):
    if not x:
        return None, False

    direction = x[0]

    if direction not in ('>', '<'):
        raise ValueError

    backwards = direction == '<'
    cells = s.unserialize_values(x[1:])
    return cells, backwards


class Page(list):
    def scalar(self):
        return self.one()[0]

    def one(self):
        c = len(self)

        if c < 1:
            raise RuntimeError('tried to select one but zero rows returned')
        elif c > 1:
            raise RuntimeError('too many rows returned')
        else:
            return self[0]

    def keys(self):
        return self._keys


class Paging(object):
    """
    Object with paging information. Most properties return a page marker. Prefix these properties with 'bookmark_' to get the serialized version of that page marker. Naming conventions are as follows:

    Ordering as returned by the query
    ---------------------------------

    - 0: the key used in the where clause
    - 1: the key of the first row returned
    - n: the key of the nth row returned
    - nplus1: the marker of the row returned beyond n
    - further: the direction continuing in this order


    Ordering once flipped if necessary (ie for backwards-facing pages)
    ------------------------------------------------------------------

    - next: the next page
    - previous: the previous page

    - current_forward: the marker
    - current_backward: the marker for this page going backwards
    - current: the marker as actually used
    - current_opposite: the marker for the same page in the opposite direction


    Tests
    -----

    - has_next: True if there's more rows after this page.
    - has_previous: True if there's more rows before this page.
    - has_further: True if there's more rows in the paging direction.

    """

    def __init__(
            self,
            rows,
            per_page,
            ocols,
            backwards,
            current_marker,
            get_marker,
            keys=None):

        self._keys = keys

        self.original_rows = rows

        self.per_page = per_page
        self.backwards = backwards

        excess = rows[per_page:]
        rows = rows[:per_page]
        self.rows = rows

        self.marker_0 = current_marker

        if rows:
            self.marker_1 = get_marker(rows[0], ocols)
            self.marker_n = get_marker(rows[-1], ocols)
        else:
            self.marker_1 = None
            self.marker_n = None

        if excess:
            self.marker_nplus1 = get_marker(excess[0], ocols)
        else:
            self.marker_nplus1 = None

        four = [self.marker_0, self.marker_1, self.marker_n, self.marker_nplus1]

        if backwards:
            self.rows.reverse()
            four.reverse()

        self.before, self.first, self.last, self.beyond = four

    @property
    def has_next(self):
        return bool(self.beyond)

    @property
    def has_previous(self):
        return bool(self.before)

    @property
    def next(self):
        return (self.last or self.before), False

    @property
    def previous(self):
        return (self.first or self.beyond), True

    @property
    def current_forwards(self):
        return self.before, False

    @property
    def current_backwards(self):
        return self.beyond, True

    @property
    def current(self):
        if self.backwards:
            return self.current_backwards
        else:
            return self.current_forwards

    @property
    def current_opposite(self):
        if self.backwards:
            return self.current_forwards
        else:
            return self.current_backwards

    @property
    def further(self):
        if self.backwards:
            return self.previous
        else:
            return self.next

    @property
    def has_further(self):
        if self.backwards:
            return self.has_previous
        else:
            return self.has_next

    @property
    def is_full(self):
        return len(self.rows) == self.per_page

    def __getattr__(self, name):
        PREFIX = 'bookmark_'
        if name.startswith(PREFIX):
            _, attname = name.split(PREFIX, 1)
            x = getattr(self, attname)
            return serialize_bookmark(x)

        raise AttributeError
