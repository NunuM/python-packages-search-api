# Python Packages Search Api


With search python packages REST API you can find your package by name ordered by relevance. The data comes from PiPy (pip) and it's updated every day. As you might know, PyPy XMLRPC search API was disabled, now, we offer you this alternative. 

### Indexer

The names of the packages are stored in a virtual table using [SQLite FFS5](https://www.sqlite.org/fts5.html), like this, we can execute full-text searches. When you search a package, we use this table to obtain the best results, from the result set, we query the package metadata table and return the packages information.

This package metadata table is a cold table, meaning, that it starts empty and for each query you do, we get the full-text results, from these results, the indexer will fetch and insert the metadata if the package metadata table does not contain the packages. So, the first time could be a little be slower but the second time for the same query, it will be very fast.

The not having stale data on the package metadata table, we update the row with the refreshed metadata after one month, in other words, each row will be updated monthly if it is queried every day.
