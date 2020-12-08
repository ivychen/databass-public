"""
Implementation of logical and physical relational operators
"""
from ..baseops import UnaryOp
from ..exprs import *
from ..schema import *
from ..tuples import *
from ..db import Mode
from ..util import cache, OBTuple
from itertools import chain

########################################################
#
# Source Operators
#
########################################################


class Source(UnaryOp):
  pass

class SubQuerySource(Source):
  """
  Allows subqueries in the FROM clause of a query
  Mainly responsible for giving the subquery an alias
  """
  def __init__(self, c, alias=None):
    super(SubQuerySource, self).__init__(c)
    self.alias = alias 

  def __iter__(self):
    for row in self.c:
      yield row

  def init_schema(self):
    """
    A source operator's schema should be initialized with the same 
    tablename as the operator's alias
    """
    self.schema = self.c.schema.copy()
    self.schema.set_tablename(self.alias)
    return self.schema


class DummyScan(Source):
  def __iter__(self):
    yield ListTuple(Schema([]))

  def init_schema(self):
    self.schema = Schema([])
    return self.schema

  def __str__(self):
    return "DummyScan()"


class Scan(Source):
  """
  A scan operator over a table in the Database singleton.
  """
  def __init__(self, tablename, alias=None):
    super(Scan, self).__init__()
    self.tablename = tablename
    self.alias = alias or tablename

    from ..db import Database
    self.db = Database.db()

  def init_schema(self):
    """
    A source operator's schema should be initialized with the same 
    tablename as the operator's alias
    """
    self.schema = self.db.schema(self.tablename).copy()
    self.schema.set_tablename(self.alias)
    print("schema: ", self.schema)
    return self.schema

  def __iter__(self):
    # initialize a single intermediate tuple
    irow = ListTuple(self.schema, [])

    for row in self.db[self.tablename]:
      irow.row = row.row
      yield irow

  def __str__(self):
    return "Scan(%s AS %s)" % (self.tablename, self.alias)


class ScanWithProject(Source):
  def __init__(self, tablename, exprs, aliases=[], alias=None):
    super(ScanWithProject, self).__init__()
    self.tablename = tablename
    self.alias = alias or tablename
    self.exprs = exprs
    self.aliases = aliases

    from ..db import Database
    self.db = Database.db()

  def init_schema(self):
    """
    A source operator's schema should be initialized with the same 
    tablename as the operator's alias
    """
    # print("table schema: ", self.db.schema(self.tablename))
    self.schema = Schema([])
    for alias, expr in zip(self.aliases, self.exprs):
      typ = expr.get_type()
      self.schema.attrs.append(Attr(alias, typ))
    self.schema.set_tablename(self.alias)
    return self.schema

  def __iter__(self):
    # initialize a single intermediate tuple
    irow = ListTuple(self.schema, [])
    # print(irow)
    if self.db.mode == Mode.COLUMN_ALL:
      for row_index in range(len(self.db[self.tablename])):
        for i, exp in enumerate(self.exprs):
          col_index = exp.aname
          print(row_index, col_index)
          val = self.db[self.tablename][(row_index, col_index)]
          irow.row[i] = val
        yield irow
    else:
      # Override ColumnTable iterator to accept schema as parameter, then read
      # only select columns from disk.
      for row in self.db[self.tablename]:
        for i, (exp) in enumerate(self.exprs):
          irow.row[i] = exp(row)
        yield irow

  def __str__(self):
    return "ScanWithProject(%s AS %s)" % (self.tablename, self.alias)


class TableFunctionSource(UnaryOp):
  """
  Scaffold for a table UDF function that outputs a relation.
  Not implemented.
  """
  def __init__(self, function, alias=None):
    super(TableFunctionSource, self).__init__(function)
    self.function = function
    self.alias = alias 

  def __iter__(self):
    raise Exception("TableFunctionSource: Not implemented")

  def __str__(self):
    return "TableFunctionSource(%s)" % self.alias



