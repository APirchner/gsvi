GSVI - Google Search Volume Index
*********************************

A interface for the `Google Trends <https://trends.google.com/>`_ time-series widget.

Motivation
==========
The Google Search Volume Index (GSVI) is a useful metric for the
attention a certain topic (product, event etc.) receives.
Fetching data from Google Trends automatically can be a pain.
This package makes getting arbitrary-length time-series of monthly, daily and hourly data as easy as::

    connection = GoogleConnection()
    series = SVSeries.univariate(connection=connection,
                                 query={'key': 'sp500', 'geo': 'US'},
                                 start=start, end=end,
                                 category=CategoryCodes.FINANCE,
                                 granularity='DAY'
                                 )
    ts = series.get_data()


Idea
====
Google Trends' quota limits are a big factor slowing down the collection of data.
Packing requests tightly reduced the load on GT and pushes the limits
on data one can pull before getting kicked -
this package bundles up to queries into one request.
Another weird quirks of Google Trends are the data normalization and granularity.
GT reports daily data for queries with an interval length between 1 and 269 days,
hourly data between  3 and 7 days and
monthly data from 1890 days on (NOTE: this could change at any time!).
For each request, the volume within one request is normalized to have a maximal value of 100.
So the query for the daily/hourly search volume for keyword abc in some interval, is handled by *gsvi* as follows:

  #. Bundle the query into requests of max. 5 269-day (7-day for hourly) fragments each and
     get the SV from Google Trends. If this is sufficient to cover the whole interval, skip the next steps.
  #. Look for the query holding the maximum (100) in each request.
  #. Bundle the fragments holding the maxima into requests of 5 each and get the SV from Google Trends.
  #. Repeat 2.-3. until only one interval holding the global maximum is left
  #. Bundle the original query into requests of 4 269-day (7-day) fragments +
     fragment with global maximum get the SV from Google Trends.
 
This procedure results in a continuous series that is normalized to
\[0, 100\] over the global maximum by Google Trends itself.
For monthly data, one request per key is sufficient.

This idea naturally extends to multivariate series of up to n queries,
limited only by what GT allows you to pull.


Usage
=====
For example, we would like to get the daily search volume for 'food' in the
U.S. from 2014-09-13 to 2019-06-13::

    import datetime as dt
    from gsvi.connection import GoogleConnection
    from gsvi.timeseries import SVSeries

    # series start and end
    start = dt.datetime(year=2014, month=9, day=13)
    end = dt.datetime(year=2019, month=6, day=13)

    # make connection to Google Trends and inject connection into the request structure
    connection = GoogleConnection()
    series = SVSeries.univariate(connection=connection,
                                 query={'key': 'food', 'geo': 'US'},
                                 start=start, end=end, granularity='DAY'
                                 )
    ts = series.get_data()

Or, what about the monthly volume of car brand searches in Germany from 2004 to June 2019::

    import datetime as dt

    from gsvi.connection import GoogleConnection
    from gsvi.timeseries import SVSeries

    # series start and end
    start = dt.datetime(year=2014, month=9, day=13)
    end = dt.datetime(year=2019, month=6, day=13)

    # make connection to Google Trends and inject connection into the request structure
    connection = GoogleConnection()
    query_multi = [{'key': 'mercedes', 'geo': 'DE'},
                  {'key': 'porsche', 'geo': 'DE'},
                  {'key': 'bmw', 'geo': 'DE'},
                  {'key': 'audi', 'geo': 'DE'},
                  {'key': 'vw', 'geo': 'DE'},
                  {'key': 'ford', 'geo': 'DE'},
                  ]
    start_multi = dt.datetime(year=2004, month=1, day=1)
    end_multi = dt.datetime(year=2019, month=6, day=1)
    series = SVSeries.multivariate(connection=connection,
                                   queries=query_multi,
                                   start=start, end=end,
                                   granularity='MONTH'
                                  )
    ts = series.get_data()



Installation
============

::

$ pip install gsvi


Credits
=======

- The main idea for the normalization was taken from a paper by `Christopher Fink and Thomas Johann (2014) <https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2139313>`_
- The connection-handling was inspired by the `pytrends package <https://github.com/GeneralMills/pytrends>`_
