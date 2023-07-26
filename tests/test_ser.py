from __future__ import unicode_literals

import csv
import uuid
import decimal
import datetime
from pathlib import Path
import pytz

from pytest import raises
from sqlakeyset.results import s
from sqlakeyset.serial import (
    PageSerializationError,
    BadBookmark,
    UnregisteredType,
    ConfigurationError,
    escape,
    unescape,
)

utc = pytz.utc


class Z(str):
    pass


def unser_z(x):
    return x[::-1]


def ser_z(x):
    return "z", x[::-1]


s.serializers[Z] = ser_z
s.deserializers["z"] = unser_z


class Y(str):
    pass


def reversestr(x):
    return x[::-1]


s.register_type(Y, "y", reversestr, reversestr)


NAUGHTY = (Path(__file__).parent / "blns.txt").read_text().splitlines()


def test_ser():
    STRINGS = [
        "abc",
        r"abc\~",
        "~".join(["\\", "~"]),
        r"abc~1234\~1234",
        "~~~~~~~\\\\\\\\`````\\\\\\\\\\``\\`'",
    ]

    assert s.split(s.join(STRINGS)) == STRINGS
    assert s.split(s.join(NAUGHTY)) == NAUGHTY


def test_escape():
    assert escape("hello world") == "hello world"
    assert escape("hello\nworld") == r"hello\nworld"
    assert escape(r"hello\nworld") == r"hello\\nworld"
    assert escape("hello\\n\nworld\n") == r"hello\\n\nworld\n"


def test_unescape():
    assert "hello world" == unescape("hello world")
    assert "hello\nworld" == unescape(r"hello\nworld")
    assert r"hello\nworld" == unescape(r"hello\\nworld")
    assert "hello\\n\nworld\n" == unescape(r"hello\\n\nworld\n")


def test_register_type_twice():
    with raises(ConfigurationError):
        s.register_type(Y, "y", str, str)


def test_bad_serializer():
    class Q(str):
        pass

    def fail(x):
        raise Exception()

    s.register_type(Q, "q", str, fail)
    with raises(PageSerializationError):
        s.serialize_value(Q())


def test_subclass():
    class SubclassedUUID(uuid.UUID):  # such as asyncpg.pgproto.pgproto.UUID
        pass

    _uuid = s.serialize_value(SubclassedUUID("939d4cc9-830d-4cca-bd74-3ec3d541a9b3"))
    assert _uuid == "uuid:939d4cc9-830d-4cca-bd74-3ec3d541a9b3"


def test_subclass_mro_order():
    class A:
        pass

    class B:
        pass

    class C(A, B):
        pass

    class D(C, B):
        pass

    def a(x):
        return "a"

    def b(x):
        return "b"

    s.register_type(A, code="suba", serializer=a)
    s.register_type(B, code="subb", serializer=b)

    assert s.serialize_value(D()) == "suba:a"

    def c(x):
        return "c"

    s.register_type(C, code="subc", serializer=c)

    assert s.serialize_value(D()) == "subc:c"


def test_serial():
    assert s.serialize_value(None) == "x"
    assert s.serialize_value(True) == "true"
    assert s.serialize_value(False) == "false"
    assert s.serialize_value(5) == "i:5"
    assert s.serialize_value(5.0) == "f:5.0"
    assert s.serialize_value(decimal.Decimal("5.5")) == "n:5.5"
    assert s.serialize_value("abc") == "s:abc"
    assert s.serialize_value("hello\nworld") == r"s:hello\nworld"
    assert s.serialize_value("hello\\nworld") == r"s:hello\\nworld"
    assert s.serialize_value(b"abc") == "b:YWJj"
    assert s.serialize_value(b"abc") == "b:YWJj"
    assert s.serialize_value(Z("abc")) == "z:cba"
    assert s.serialize_value(Y("abc")) == "y:cba"
    assert s.serialize_value(datetime.date(2007, 12, 5)) == "d:2007-12-05"

    dt = s.serialize_value(datetime.datetime(2007, 12, 5, 12, 30, 30, tzinfo=utc))
    assert dt == "dt:2007-12-05 12:30:30+00:00"

    assert s.serialize_value(datetime.time(12, 34, 56)) == "t:12:34:56"

    _uuid = s.serialize_value(uuid.UUID("939d4cc9-830d-4cca-bd74-3ec3d541a9b3"))
    assert _uuid == "uuid:939d4cc9-830d-4cca-bd74-3ec3d541a9b3"

    with raises(UnregisteredType):
        s.serialize_value(csv.reader)


def test_unserial():
    def twoway(x):
        assert s.unserialize_value(s.serialize_value(x)) == x

    twoway(None)
    twoway(True)
    twoway(False)
    twoway(5)
    twoway(5.0)
    twoway(decimal.Decimal("5.5"))
    twoway("abc")
    twoway(b"abc")
    twoway(b"abc")
    twoway(datetime.date(2007, 12, 5))
    twoway(datetime.datetime(2007, 12, 5, 12, 30, 30, tzinfo=utc))
    twoway(Z("abc"))
    twoway(Y("abc"))
    twoway(uuid.UUID("939d4cc9-830d-4cca-bd74-3ec3d541a9b3"))
    twoway("hello\nworld")
    twoway("hello\\nworld")

    with raises(BadBookmark):
        s.unserialize_value("zzzz:abc")


def test_serial_row():
    assert s.serialize_values([None, True, False]) == "x~true~false"
    assert s.serialize_values([5, 5.0]) == "i:5~f:5.0"
    assert s.serialize_values(["hello", "world"]) == "s:hello~s:world"
    # The backslash in \n will be doubly-escaped in full row serializations:
    #  Once (by us) to handle newlines
    #  A second time (by csv.writer) to handle delimiters
    assert s.serialize_values(["hello\nworld"]) == r"s:hello\\nworld"
    assert s.serialize_values(["hello\\n\nworld"]) == r"s:hello\\\\n\\nworld"


def test_unserial_row():
    def twoway(x):
        assert s.unserialize_values(s.serialize_values(x)) == tuple(x)

    twoway([None, True])
    twoway(["hello", "world", 13])
    twoway(["hello\nworld", 13])
    twoway(["hello\\nworld", 13])
