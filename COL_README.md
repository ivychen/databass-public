## Databass: column store

## Architecture

### columns.py
  
ColumnTuple - Class definition for an in-memory column. This is similar in
function to ListTuple (in-memory row).


### tables.py

InMemoryColumnTable - Class definition for an  in-memory columnar table. The API
is similar to row-based in-memory table.


### COL_TESTING.py

Testing file for column store.

### db.py

The Database manages the catalog of tables that can be queried. It essentially
maps a table name to the Table object. The database instance can be configured
to operate as as column store by passing a flag or parameter.

## Simple column format

## Getting started