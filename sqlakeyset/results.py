"""Paging data structures and bookmark handling."""
from __future__ import unicode_literals

import csv

from .serial import Serial, BadBookmark

SERIALIZER_SETTINGS = dict(
    lineterminator=str(""),
    delimiter=str("~"),
    doublequote=False,
    escapechar=str("\\"),
    quoting=csv.QUOTE_NONE,
)

s = Serial(**SERIALIZER_SETTINGS)


def custom_bookmark_type(type, code, deserializer=None, serializer=None):
    """Register (de)serializers for bookmarks to use for a custom type.

    :param type: Python type to register.
    :paramtype type: type
    :param code: A short alphabetic code to use to identify this type in serialized bookmarks.
    :paramtype code: str
    :param serializer: A function mapping `type` values to strings. Default is
        `str`.
    :param deserializer: Inverse for `serializer`. Default is the `type`
        constructor."""
    s.register_type(type, code, deserializer=deserializer, serializer=serializer)


def serialize_bookmark(marker):
    """Serialize a place marker to a bookmark string.

    :param marker: A pair ``(keyset, backwards)``, where ``keyset`` is a tuple
        containing values of the ordering columns, and `backwards` denotes the
        paging direction.
    :returns: A CSV-like string using ``~`` as a separator."""
    if marker is None:
        return None
    x, backwards = marker
    ss = s.serialize_values(x)
    direction = "<" if backwards else ">"
    return direction + ss


def unserialize_bookmark(bookmark):
    """Deserialize a bookmark string to a place marker.

    :param bookmark: A string in the format produced by
        :func:`serialize_bookmark`.
    :returns: A marker pair as described in :func:`serialize_bookmark`.
    """
    if not bookmark:
        return None, False

    direction = bookmark[0]

    if direction not in (">", "<"):
        raise BadBookmark(
            "Malformed bookmark string: doesn't start with a direction marker"
        )

    backwards = direction == "<"
    cells = s.unserialize_values(bookmark[1:])  # might raise BadBookmark
    return cells, backwards


class Page(list):
    """A :class:`list` of result rows with access to paging information and
    some convenience methods."""

    def __init__(self, iterable, paging=None, keys=None):
        super().__init__(iterable)
        self.paging = paging
        """The :class:`Paging` information describing how this page relates to the
       whole resultset."""
        self._keys = keys

    def scalar(self):
        """Assuming paging was called with ``per_page=1`` and a single-column
        query, return the single value."""
        return self.one()[0]

    def one(self):
        """Assuming paging was called with ``per_page=1``, return the single
        row on this page."""
        c = len(self)

        if c < 1:
            raise RuntimeError("tried to select one but zero rows returned")
        elif c > 1:
            raise RuntimeError("too many rows returned")
        else:
            return self[0]

    def keys(self):
        """Equivalent of :meth:`sqlalchemy.engine.ResultProxy.keys`: returns
        the list of string keys for rows."""
        return self._keys


class Paging:
    """Metadata describing the position of a page in a collection.
    Most properties return a page marker.
    Prefix these properties with ``bookmark_`` to get the serialized version of
    that page marker.

    Unless you're extending sqlakeyset you should not be constructing this
    class directly - use sqlakeyset.get_page or sqlakeyset.select_page to
    acquire a Page object, then access page.paging to get the paging
    metadata.
    """

    def __init__(
        self,
        rows,
        per_page,
        ocols,
        backwards,
        current_marker,
        markers=None,
        get_keys_from=None,  # used only in unit tests
    ):
        self.original_rows = rows

        if get_keys_from:

            def _get_keys_at(i):
                return get_keys_from(self.original_rows[i], ocols)

            marker = _get_keys_at
        else:
            if rows and not markers:
                raise ValueError
            marker = markers.__getitem__

        self.per_page = per_page
        self.backwards = backwards
        self._get_keys_at = marker

        excess = rows[per_page:]
        rows = rows[:per_page]
        self.rows = rows
        self.marker_0 = current_marker

        if rows:
            self.marker_1 = marker(0)
            self.marker_n = marker(len(rows) - 1)
        else:
            self.marker_1 = None
            self.marker_n = None

        if excess:
            self.marker_nplus1 = marker(len(rows))
        else:
            self.marker_nplus1 = None

        four = [self.marker_0, self.marker_1, self.marker_n, self.marker_nplus1]

        if backwards:
            if markers is not None:
                markers.reverse()  # used by _get_keys_at
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
        """Marker for the current page in the current paging direction."""
        if self.backwards:
            return self.current_backwards
        else:
            return self.current_forwards

    @property
    def current_opposite(self):
        """Marker for the current page in the opposite of the current
        paging direction."""
        if self.backwards:
            return self.current_forwards
        else:
            return self.current_backwards

    @property
    def further(self):
        """Marker for the following page in the current paging direction."""
        if self.backwards:
            return self.previous
        else:
            return self.next

    @property
    def has_further(self):
        """Boolean flagging whether there are more rows before this page in the
        current paging direction."""
        if self.backwards:
            return self.has_previous
        else:
            return self.has_next

    @property
    def is_full(self):
        """Boolean flagging whether this page contains as many rows as were
        requested in ``per_page``."""
        return len(self.rows) == self.per_page

    def get_marker_at(self, i):
        """Get the marker for item at the given row index."""
        return self._get_keys_at(i), self.backwards

    def get_bookmark_at(self, i):
        """Get the bookmark for item at the given row index."""
        return serialize_bookmark(self.get_marker_at(i))

    def items(self):
        """Iterates over the items in the page, returning a tuple ``(marker,
        item)`` for each."""
        for i, row in enumerate(self.rows):
            yield self.get_marker_at(i), row

    def bookmark_items(self):
        """Iterates over the items in the page, returning a tuple ``(bookmark,
        item)`` for each."""
        for i, row in enumerate(self.rows):
            yield self.get_bookmark_at(i), row

    # The remaining properties are just convenient shorthands to avoid manually
    # calling serialize_bookmark.
    @property
    def bookmark_next(self):
        """Bookmark for the next page (in the original query order)."""
        return serialize_bookmark(self.next)

    @property
    def bookmark_previous(self):
        """Bookmark for the previous page (in the original query order)."""
        return serialize_bookmark(self.previous)

    @property
    def bookmark_current_forwards(self):
        """Bookmark for the current page in forwards direction."""
        return serialize_bookmark(self.current_forwards)

    @property
    def bookmark_current_backwards(self):
        """Bookmark for the current page in backwards direction."""
        return serialize_bookmark(self.current_backwards)

    @property
    def bookmark_current(self):
        """Bookmark for the current page in the current paging direction."""
        return serialize_bookmark(self.current)

    @property
    def bookmark_current_opposite(self):
        """Bookmark for the current page in the opposite of the current
        paging direction."""
        return serialize_bookmark(self.current_opposite)

    @property
    def bookmark_further(self):
        """Bookmark for the following page in the current paging direction."""
        return serialize_bookmark(self.further)
