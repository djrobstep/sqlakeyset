from __future__ import unicode_literals

import decimal
import datetime
import base64
import dateutil.parser

from .compat import csvreader, csvwriter, sio, text_type, binary_type


NONE = 'x'
TRUE = 'true'
FALSE = 'false'
STRING = 's'
BINARY = 'b'
INTEGER = 'i'
FLOAT = 'f'
DECIMAL = 'n'
DATE = 'd'
DATETIME = 'dt'
TIME = 't'


class Serial(object):
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        self.custom_serializations = {}
        self.custom_unserializations = {}

    def split(self, joined):
        s = sio(joined)
        r = csvreader(s, **self.kwargs)
        row = next(r)
        return row

    def join(self, string_list):
        s = sio()
        w = csvwriter(s, **self.kwargs)
        w.writerow(string_list)
        return s.getvalue()

    def serialize_values(self, values):
        if values is None:
            return ''
        return self.join(self.serialize_value(_) for _ in values)

    def unserialize_values(self, s):
        if s == '':
            return None

        return [self.unserialize_value(_) for _ in self.split(s)]

    def serialize_value(self, x):
        if x is None:
            return NONE
        elif x is True:
            return TRUE
        elif x is False:
            return FALSE

        t = type(x)

        if t in self.custom_serializations:
            c, x = self.custom_serializations[t](x)
        elif t == text_type:
            c = STRING
        elif t == binary_type:
            c = BINARY
            x = base64.b64encode(x).decode('utf-8')
        elif t == int:
            c = INTEGER
        elif t == float:
            c = FLOAT
        elif t == decimal.Decimal:
            c = DECIMAL
        elif t == datetime.date:
            c = DATE
        elif t == datetime.datetime:
            c = DATETIME
        elif t == datetime.time:
            c = TIME
        else:
            raise NotImplementedError(
                "don't know how to serialize type of {} ({})".format(x, type(x)))

        return '{}:{}'.format(c, x)

    def unserialize_value(self, x):
        try:
            c, v = x.split(':', 1)
        except ValueError:
            c = x
            v = None

        if c in self.custom_unserializations:
            return self.custom_unserializations[c](v)
        elif c == NONE:
            return None
        elif c == TRUE:
            return True
        elif c == FALSE:
            return False
        elif c == STRING:
            pass
        elif c == BINARY:
            v = base64.b64decode(v.encode('utf-8'))
        elif c == INTEGER:
            v = int(v)
        elif c == FLOAT:
            v = float(v)
        elif c == DECIMAL:
            v = decimal.Decimal(v)
        elif c == DATE:
            v = dateutil.parser.parse(v)
            v = v.date()
        elif c == DATETIME:
            v = dateutil.parser.parse(v)
        else:
            raise ValueError('unrecognized value {}'.format(x))

        return v
