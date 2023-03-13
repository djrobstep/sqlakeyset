"""Methods for messing with the internals of SQLAlchemy >=2.0 results."""
from .sqla14_aux import *


def core_coerce_row(row, extra_columns, result_type):
    """Trim off the extra columns and return as a correct-as-possible
    sqlalchemy Row."""
    if not extra_columns:
        return row
    N = len(row) - len(extra_columns)

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
