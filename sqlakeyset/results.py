"""Paging data structures and bookmark handling."""
import base64
import csv
from typing import Any, Optional

from sqlakeyset.serial import BadBookmark, Serial

SERIALIZER_SETTINGS = {
    "lineterminator": "",
    "delimiter": "~",
    "doublequote": False,
    "escapechar": "\\",
    "quoting": csv.QUOTE_NONE,
}

s = Serial(**SERIALIZER_SETTINGS)




def serialize_bookmark(marker: tuple[tuple[Any], bool]) -> str:
    """
    Serialize the given bookmark.

    Args:
        marker: A pair `(keyset, backwards)`, where ``keyset`` is a tuple containing values of the ordering columns,
                and `backwards` denotes the paging direction.

    Returns:
        A serialized string.
    """
    x, backwards = marker
    ss = s.serialize_values(x)
    direction = "<" if backwards else ">"
    full_string = direction + ss
    return base64.b64encode(full_string.encode()).decode()


def unserialize_bookmark(bookmark: Optional[str]) -> tuple[Optional[tuple[Any]], bool]:
    """
    Deserialize a bookmark string to a place marker.

    Args:
        bookmark: A string in the format produced by :func:`serialize_bookmark`.

    Returns:
        A marker pair as described in :func:`serialize_bookmark`.

    Raises:
        BadBookmark: The bookmark is not a valid.
    """
    if not bookmark:
        return None, False

    decoded = base64.b64decode(bookmark.encode()).decode()

    direction = decoded[0]

    if direction not in (">", "<"):
        raise BadBookmark("Malformed bookmark string: doesn't start with a direction marker")

    backwards = direction == "<"
    cells = s.unserialize_values(decoded[1:])  # might raise BadBookmark
    return cells, backwards


class Page(list):
    """
    A :class:`list` of result rows with access to paging information and
    some convenience methods.
    """

    def __init__(self, iterable, paging: "Paging", keys=None):
        super().__init__(iterable)
        self.paging = paging
        """The :class:`Paging` information describing how this page relates to the
       whole resultset."""
        self._keys = keys

    def scalar(self):
        """
        Assuming paging was called with ``per_page=1`` and a single-column
        query, return the single value.
        """
        return self.one()[0]

    def one(self):
        """
        Assuming paging was called with ``per_page=1``, return the single
        row on this page.

        Raises:
            Exception: ???
        """
        c = len(self)

        if c < 1:
            raise Exception("tried to select one but zero rows returned")
        elif c > 1:
            raise Exception("too many rows returned")
        else:
            return self[0]

    def keys(self):
        """
        Equivalent of :meth:`sqlalchemy.engine.ResultProxy.keys`: returns
        the list of string keys for rows.
        """
        return self._keys


class Paging:
    """
    Object with paging information. Most properties return a page marker.
    Prefix these properties with ``bookmark_`` to get the serialized version of
    that page marker.
    """

    def __init__(
        self,
        rows,
        per_page,
        ocols,
        backwards,
        current_marker,
        get_marker=None,
        markers=None,
    ):

        self.original_rows = rows

        if get_marker:

            def _get_marker(i):
                return get_marker(self.original_rows[i], ocols)

            marker = _get_marker
        else:
            if rows and not markers:
                raise ValueError
            marker = markers.__getitem__

        self.per_page = per_page
        self.backwards = backwards

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
            self.rows.reverse()
            four.reverse()

        self.before, self.first, self.last, self.beyond = four

    @property
    def has_next(self):
        """
        Boolean flagging whether there are more rows after this page (in the
        original query order).
        """
        return bool(self.beyond)

    @property
    def has_previous(self):
        """
        Boolean flagging whether there are more rows before this page (in the
        original query order).
        """
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
        """
        Marker for the current page in the opposite of the current
        paging direction.
        """
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
        """
        Boolean flagging whether there are more rows before this page in the
        current paging direction.
        """
        if self.backwards:
            return self.has_previous
        else:
            return self.has_next

    @property
    def is_full(self):
        """
        Boolean flagging whether this page contains as many rows as were
        requested in ``per_page``.
        """
        return len(self.rows) == self.per_page

    def __getattr__(self, name):
        prefix = "bookmark_"
        if name.startswith(prefix):
            _, attname = name.split(prefix, 1)
            x = getattr(self, attname)
            return serialize_bookmark(x)

        raise AttributeError
