import pandas
import numbers
import os
from .stats import Stats, TableType
from .tuples import *
from .columns import * # ColumnTuple
from .exprs import Attr
from .schema import Schema

class Table(object):
  """
  A table consists of a schema, and a way to iterate over the rows.
  Specific subclasses can enforce the specific row representations they want 
  e.g., columnar, row-wise, bytearrays, indexes, etc
  """
  id = 0

  def __init__(self, schema):
    self.schema = schema
    self.id = Table.id
    Table.id += 1

    self._stats = None


  @staticmethod
  def from_rows(rows):
    if not rows:
      return InMemoryTable(Schema([]), rows)
    schema = Table.schema_from_rows(list(rows[0].keys()), rows)
    return InMemoryTable(schema, rows)

  @staticmethod
  def from_columns(columns):
    if not columns:
      return InMemoryColumnTable(Schema([]), columns)
    # TODO(ic2389): Build schema. Construct and return in-memory column table.

  @property
  def stats(self):
    if self._stats is None:
      self._stats = Stats(self)
    return self._stats

  def col_values(self, field):
    idx = self.schema.idx(Attr(field.aname))
    return [row[idx] for row in self]

  def __iter__(self):
    yield


class InMemoryTable(Table):
  """
  Row-oriented table that stores its data as an array in memory.
  """
  def __init__(self, schema, rows):
    super(InMemoryTable, self).__init__(schema)
    self._type = TableType.ROW
    self.rows = rows
    self.attr_to_idx = { a.aname: i 
        for i,a in enumerate(self.schema)}
    # Populates col_stats with stats for each attribute in the schema
    self.stats
    for attr in self.schema:
      self.stats[attr]

  def __iter__(self):
    # Iterate through each row in table
    for row in self.rows:
      yield ListTuple(self.schema, row)

  @property
  def type(self):
    return self._type

class InMemoryColumnTable(Table):
  """
  Column-oriented table that stores data in-memory in columnar format.

  Attributes:
  
  schema(Schema) - this defined the schema for the table
  columns(list(ColumnTuple)) - list of ColumnTuples
  """
  def __init__(self, schema, columns):
    super(InMemoryColumnTable, self).__init__(schema)
    self._type = TableType.COLUMN
    self.columns = columns
    self.attr_to_idx = { a.aname: i for i,a in enumerate(self.schema) }
    # maps index to attribute
    self.idx_to_attr = {i: a for i, a in enumerate(self.schema)}
    # Populates col_stats with stats for each attribute in the schema
    self.stats
    for attr in self.schema:
      self.stats[attr]

  def __iter__(self):
    # TODO: READ SUBSET OF COLUMNS FROM DISK COLUMN FILES
    for i in range(len(self.columns[0])):
      row = []
      for cols in self.columns:
        row.append(cols[i])
      yield ListTuple(self.schema, row)
    # for idx, column in enumerate(self.columns):
    #   col_schema = Schema([])
    #   attr = self.idx_to_attr[idx]
    #   col_schema.attrs.append(attr)
    #   yield ColumnTuple(col_schema, column)

  @property
  def type(self):
    return self._type