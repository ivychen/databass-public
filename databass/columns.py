"""
Classes for column data format.
"""

from .exprs import *

class ColumnTuple(object):
  """
  Represents a column in columnar store. A column consists of a schema
  definition encapsulating a single column name and data type.
  See exprs.py#Attr for details on the schema definition.

  This is similar to the ListTuple, but ListTuple is an in-memory row.

  Attributes:
  schema(Schema) - the column schema containing name and data type
  column(list) - a list of data values stored in memory
  """
  def __init__(self, schema, column=None):
    self.schema = schema
    self.column = column or []

  def copy(self):
    return ColumnTuple(self.schema.copy(), list(self.column))

  def __hash__(self):
    return hash(str(self.column))

  def __getitem__(self, idx):
    return self.column[idx]

  def __setitem_(self, idx, val):
    self.row[idx] = val

  def __str__(self):
    return "(%s)" % ", ".join(map(str, self.column))
