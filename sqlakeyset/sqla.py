import sqlalchemy
from packaging import version

if version.parse(sqlalchemy.__version__) < version.parse("1.4.0b1"):
    from .sqla13 import *  # noqa
else:
    from .sqla14 import *  # noqa
