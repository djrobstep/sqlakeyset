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
    from .sqla13 import *  # noqa
elif SQLA_VERSION < version.parse("2.0"):
    from .sqla14 import *  # noqa
else:
    from .sqla20 import *  # noqa
