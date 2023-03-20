import sqlalchemy
from packaging import version


SQLA_VERSION = version.parse(sqlalchemy.__version__)


def get_bind(q, s):
    try:
        # session
        return s.get_bind(clause=getattr(q, "statement", q))
    except Exception:
        # connection/engine
        return s


if SQLA_VERSION < version.parse("1.4.0b1"):
    from .sqla13 import (
        core_coerce_row,
        core_result_type,
        group_by_clauses,
        order_by_clauses,
        orm_coerce_row,
        orm_query_keys,
        orm_result_type,
        orm_to_selectable,
    )
elif SQLA_VERSION < version.parse("2.0.0b1"):
    from .sqla14 import (
        core_coerce_row,
        core_result_type,
        group_by_clauses,
        order_by_clauses,
        orm_coerce_row,
        orm_query_keys,
        orm_result_type,
        orm_to_selectable,
    )
else:
    from .sqla20 import (
        core_coerce_row,
        core_result_type,
        group_by_clauses,
        order_by_clauses,
        orm_coerce_row,
        orm_query_keys,
        orm_result_type,
        orm_to_selectable,
    )

__all__ = [
    "core_coerce_row",
    "core_result_type",
    "get_bind",
    "group_by_clauses",
    "order_by_clauses",
    "orm_coerce_row",
    "orm_query_keys",
    "orm_result_type",
    "orm_to_selectable",
]
