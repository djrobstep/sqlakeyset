import io
import csv
import sys


PY2 = sys.version_info.major <= 2


def decode(x):  # pragma: no cover
    if x is None:
        return x
    return x.decode('utf-8')


def encode(x):  # pragma: no cover
    if x is None:
        return x
    return x.encode('utf-8')


class UTF8Recoder(object):  # pragma: no cover
    """
    Iterator that reads a text stream and reencodes the input to UTF-8
    """

    def __init__(self, f):
        self.f = f

    def __iter__(self):
        return self

    def next(self):
        x = self.f.next()
        return x.encode("utf-8")


class TextReader(object):  # pragma: no cover
    """
    Accepts text streams. Used for wrapping a python-2 csv `reader`
    (which reads bytes).
    """

    def __init__(self, f, dialect=csv.excel, **kwds):
        f = UTF8Recoder(f)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return list(r.decode('utf-8') for r in row)

    __next__ = next

    def __iter__(self):
        return self


class TextWriter(object):  # pragma: no cover
    """Wraps a python2 csv writer for writing to text streams
    """

    def __init__(self, f, dialect=csv.excel, **kwds):
        self.queue = io.BytesIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f

    def writerow(self, row):
        e = [s.encode('utf-8') for s in row]
        self.writer.writerow(e)
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()

        data = data.decode('utf-8')
        # ... and reencode it into the target encoding
        self.stream.write(data)
        # empty queue
        self.queue.seek(0)
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


if PY2:  # flake8: noqa
    text_type = unicode
    binary_type = str
else:
    text_type = str
    binary_type = bytes


if PY2:  # pragma: no cover
    import StringIO
    sio = StringIO.StringIO
    csvreader = TextReader
    csvwriter = TextWriter
else:  # pragma: no cover
    sio = io.StringIO
    csvreader = csv.reader
    csvwriter = csv.writer
