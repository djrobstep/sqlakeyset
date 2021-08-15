import sqlalchemy
from packaging import version


def get_bind(q, s):
    try:
        # session
        return s.get_bind(clause=getattr(q, "statement", q))
    except Exception:
        # connection/engine
        return s


if version.parse(sqlalchemy.__version__) < version.parse("1.4.0b1"):
    from .sqla13 import *  # noqa
else:
    from .sqla14 import *  # noqa
