"""Methods for messing with the internals of SQLAlchemy >1.3 results."""
from sqlalchemy.engine.row import Row as _Row, LegacyRow as _LegacyRow

from .sqla14_aux import *


class LegacyRow(_Row):
    def keys(self):
        return result_keys(self._parent)


def core_coerce_row(row, extra_columns, result_type):
    """Trim off the extra columns and return as a correct-as-possible
    sqlalchemy Row."""
    if not extra_columns:
        return row
    N = len(row) - len(extra_columns)

    if isinstance(row, _LegacyRow):
        cls = LegacyRow
    else:
        cls = Row

    return cls(
        row._parent,
        None,  # Processors are applied immediately in sqla1.4+
        {  # Strip out added OCs from the keymap:
            k: v
            for k, v in row._keymap.items()
            if not v[1].startswith(ORDER_COL_PREFIX)
        },
        row._key_style,
        row._data[:N],
    )
