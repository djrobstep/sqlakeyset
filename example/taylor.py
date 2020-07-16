from __future__ import unicode_literals

from sqlakeyset import get_page
from sqlbag import S
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, desc


SINGLES = """Tim McGraw 2006 40
Teardrops on My Guitar 2007 13
Our Song 2007 16
Picture to Burn 2008 28
Should've Said No 2008 33
Change 2008 10
Love Story 2008 4
White Horse 2008 13
You Belong with Me 2009 2
Fifteen 2009 23
Fearless 2010 9
Today Was a Fairytale 2010 2
Mine 2010 3
Back to December 2010 6
Mean 2011 11
The Story of Us 2011 41
Sparks Fly 2011 17
Ours 2011 13
Safe & Sound (featuring The Civil Wars) 2011 30
Long Live (featuring Paula Fernandes) 2012 85
Eyes Open 2012 19
We Are Never Ever Getting Back Together 2012 1
Ronan 2012 16
Begin Again 2012 7
I Knew You Were Trouble 2012 2
22 2013 20
Highway Don't Care (with Tim McGraw) 2013 22
Red 2013 6
Everything Has Changed (featuring Ed Sheeran) 2013 32
Sweeter Than Fiction 2013 34
Shake It Off 2014 1
Blank Space 2014 1
Style 2015 6
Bad Blood (featuring Kendrick Lamar) 2015 1
Wildest Dreams 2015 5
Out of the Woods 2016 18
New Romantics 2016 46"""


Base = declarative_base()


class Single(Base):
    __tablename__ = "single"
    id = Column(Integer, primary_key=True)
    title = Column(String(255))
    year = Column(Integer)
    peak_position = Column(Integer)


PER_PAGE = 3

# this database needs to exist for the example to work
DB = "postgresql:///taylor"


def print_page(p):
    print("\n\nPage for key: {}\n".format(p.paging.bookmark_current))

    for x in p:
        print("{:>4d}  {}  {}".format(x.peak_position, x.year, x.title))
    print("\n")


def main():
    with S(DB, echo=False) as s:
        s.execute(
            """
            drop table if exists single;
        """
        )

        s.execute(
            """
            create table if not exists
                single(id serial, title text, year int, peak_position int)
        """
        )

    with S(DB, echo=False) as s:
        for line in SINGLES.splitlines():
            title, year, peak = line.rsplit(" ", 2)

            single = Single(title=title, year=year, peak_position=peak)
            s.add(single)

    with S(DB, echo=False) as s:
        q = s.query(Single).order_by(
            Single.peak_position, desc(Single.year), Single.title, desc(Single.id)
        )

        bookmark = None

        while True:
            p = get_page(q, per_page=PER_PAGE, page=bookmark)
            print_page(p)
            bookmark = p.paging.bookmark_next
            if not p.paging.has_next:
                break


if __name__ == "__main__":
    main()
