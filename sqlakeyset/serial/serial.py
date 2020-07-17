"""Bookmark (de)serialization logic."""
from __future__ import unicode_literals

import decimal
import datetime
import base64
import uuid
import dateutil.parser
import csv
from io import StringIO

NONE = "x"
TRUE = "true"
FALSE = "false"
STRING = "s"
BINARY = "b"
INTEGER = "i"
FLOAT = "f"
DECIMAL = "n"
DATE = "d"
DATETIME = "dt"
TIME = "t"
UUID = "uuid"


def parsedate(x):
    return dateutil.parser.parse(x).date()


def binencode(x):
    return base64.b64encode(x).decode("utf-8")


def bindecode(x):
    return base64.b64decode(x.encode("utf-8"))


TYPES = [
    (str, "s"),
    (int, "i"),
    (float, "f"),
    (bytes, "b", bindecode, binencode),
    (decimal.Decimal, "n"),
    (uuid.UUID, "uuid"),
    (datetime.datetime, "dt", dateutil.parser.parse),
    (datetime.date, "d", parsedate),
    (datetime.time, "t"),
]

BUILTINS = {
    "x": None,
    "true": True,
    "false": False,
}
BUILTINS_INV = {v: k for k, v in BUILTINS.items()}


class Serial(object):
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        self.custom_serializations = {}
        self.custom_unserializations = {}
        for definition in TYPES:
            self.register_type(*definition)

    def register_type(self, type, code, deserializer=None, serializer=None):
        if serializer is None:
            serializer = str
        if deserializer is None:
            deserializer = type
        if type in self.custom_serializations:
            raise ValueError("Type {type} already has a serializer registered.")
        if code in self.custom_unserializations:
            raise ValueError("Type code {code} is already in use.")
        self.custom_serializations[type] = lambda x: (code, serializer(x))
        self.custom_unserializations[code] = deserializer

    def split(self, joined):
        s = StringIO(joined)
        r = csv.reader(s, **self.kwargs)
        row = next(r)
        return row

    def join(self, string_list):
        s = StringIO()
        w = csv.writer(s, **self.kwargs)
        w.writerow(string_list)
        return s.getvalue()

    def serialize_values(self, values):
        if values is None:
            return ""
        return self.join(self.serialize_value(_) for _ in values)

    def unserialize_values(self, s):
        if s == "":
            return None

        return [self.unserialize_value(_) for _ in self.split(s)]

    def serialize_value(self, x):
        try:
            c, x = self.custom_serializations[type(x)](x)
            return "{}:{}".format(c, x)
        except KeyError:
            pass

        try:
            return BUILTINS_INV[x]
        except KeyError:
            raise NotImplementedError(
                "don't know how to serialize type of {} ({})".format(x, type(x))
            )

    def unserialize_value(self, x):
        try:
            c, v = x.split(":", 1)
        except ValueError:
            c = x
            v = None

        try:
            return self.custom_unserializations[c](v)
        except KeyError:
            pass

        try:
            return BUILTINS[c]
        except KeyError:
            raise ValueError("unrecognized value {}".format(x))
