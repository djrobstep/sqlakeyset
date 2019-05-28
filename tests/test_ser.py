from __future__ import unicode_literals

import csv
import io
import uuid
import decimal
import datetime
import pytz

from pytest import raises
from sqlakeyset.serial import Serial

utc = pytz.utc

DEFAULTS = dict(
    lineterminator=str(''),
    delimiter=str('~'),
    doublequote=False,
    escapechar=str('\\'),
    quoting=csv.QUOTE_NONE)

s = Serial(**DEFAULTS)


class Z(str):
    pass


def unser_z(x):
    return x[::-1]


def ser_z(x):
    return 'z', x[::-1]


s.custom_serializations[Z] = ser_z
s.custom_unserializations['z'] = unser_z


with io.open('tests/blns.txt') as f:
    NAUGHTY = f.read().splitlines()


def test_ser():
    STRINGS = [
        'abc',
        r'abc\~',
        '~'.join(['\\', '~']),
        r'abc~1234\~1234',
        '~~~~~~~\\\\\\\\`````\\\\\\\\\\``\\`\''
    ]

    assert s.split(s.join(STRINGS)) == STRINGS
    assert s.split(s.join(NAUGHTY)) == NAUGHTY


def test_serial():
    assert s.serialize_value(None) == 'x'
    assert s.serialize_value(True) == 'true'
    assert s.serialize_value(False) == 'false'
    assert s.serialize_value(5) == 'i:5'
    assert s.serialize_value(5.0) == 'f:5.0'
    assert s.serialize_value(decimal.Decimal('5.5')) == 'n:5.5'
    assert s.serialize_value('abc') == 's:abc'
    assert s.serialize_value(b'abc') == 'b:YWJj'
    assert s.serialize_value(b'abc') == 'b:YWJj'
    assert s.serialize_value(datetime.date(2007, 12, 5)) == 'd:2007-12-05'
    assert s.serialize_value(datetime.datetime(2007, 12, 5, 12, 30, 30, tzinfo=utc)) \
        == 'dt:2007-12-05 12:30:30+00:00'
    assert s.serialize_value(datetime.time(12, 34, 56)) == 't:12:34:56'
    assert s.serialize_value(uuid.UUID('939d4cc9-830d-4cca-bd74-3ec3d541a9b3')) \
        == 'uuid:939d4cc9-830d-4cca-bd74-3ec3d541a9b3'
    with raises(NotImplementedError):
        s.serialize_value(csv.reader)


def test_unserial():
    def twoway(x):
        assert s.unserialize_value(s.serialize_value(x)) == x

    twoway(None)
    twoway(True)
    twoway(False)
    twoway(5)
    twoway(5.0)
    twoway(decimal.Decimal('5.5'))
    twoway('abc')
    twoway(b'abc')
    twoway(b'abc')
    twoway(datetime.date(2007, 12, 5))
    twoway(datetime.datetime(2007, 12, 5, 12, 30, 30, tzinfo=utc))
    twoway(Z('abc'))
    twoway(uuid.UUID('939d4cc9-830d-4cca-bd74-3ec3d541a9b3'))

    with raises(ValueError):
        s.unserialize_value('zzzz:abc')
