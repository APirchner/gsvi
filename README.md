# GSVI - Google Search Volume Index

A interface for the Google Trends <https://trends.google.com/> time-series widget.

## Motivation
The Google Search Volume Index (GSVI) is a useful metric for the
attention a certain topic (product, event etc.) receives. Unfortunately,
obtaining arbitrary-length series
of the daily/hourly GSVI is a cumbersome task - Google Trends does not
offer absolute values
but rather normalizes the volume to \[0, 100\] within intervals of
maximal 90 days. Thus one can not easily compare the
results of two different queries.

This package offers to do exactly that - it is an interface to the Google
Trends time-series widget and implements
an algorithm to obtain arbitrary-length, normalized time-series for
search terms.

## Idea
Google Trends reports daily data for queries with an interval <= 90 days
(<= 7 days for hourly data) and allows 5 queries within one request.
For each request, the volume is normalized to have a maximal value of 100.

So the query for the daily search volume for keyword **abc** in some interval,
is handled by *gsvi* as follows: 
 1. Bundle the query into requests of 5 30-day fragments each and
 get the SV from Google Trends.
 2. Look for the query holding the maximum (100) in each request.
 3. Bundle the fragments holding the maxima into requests of 5 each and get the SV from Google Trends.
 4. Repeat 2.-3. until only one interval holding the global maximum is left.
 5. Bundle the original query into requests of 4 30-day fragments +
 fragment with global maximum get the SV from Google Trends.
 
 This procedure results in a continuous series that was normalized to
 \[0, 100\] over the same maximum by Google Trends itself.

## Usage
For example, we would like to get the daily search volume for 'apple' in the
U.S from 2014-09-13 to 2019-06-13:
```python
import datetime as dt

from gsvi.google_connection import GoogleConnection
from gsvi.request_structures import TSUnivariate

# series start and end
start = dt.datetime(year=2014, month=9, day=13)
end = dt.datetime(year=2019, month=6, day=13)

# make connection to Google Trends and inject connection into the request structure
connection = GoogleConnection()
series = TSUnivariate(connection=connection, query={'key': 'apple', 'geo': 'US'},
                  start=start, end=end, granularity='DAY'
                  )
ts = series.get_data()
```



## Credits

- The main idea for the normalization was taken from a paper by Christopher Fink and Thomas Johann (2014) <https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2139313>
- The connection-handling was inspired by the pytrends package <https://github.com/GeneralMills/pytrends>
