from .util import guess_type
from .schema import Schema
from .tables import *
from .columns import ColumnTuple
import pandas
import numbers
import os
import enum

openfile = open

"""
Mode to initialize the database.

ROW = default row-store
COLUMN_ALL = column-store, loads all columns into memory
COLUMN_SELECT = column-store, load only some columns into memory
"""
class Mode(enum.Enum):
  ROW = 1 # Loads all rows into memory
  COLUMN_ALL = 2 # Loads all columns into memory
  COLUMN_SELECT = 3 # Load only select columns into database

"""
Each subsequent phase incorporates the previous phase.
"""
class Phase(enum.Enum):
  COL_FORMAT = 1
  COL_COMPRESSION = 2

def infer_schema_from_df(df):
  from .exprs import guess_type, Attr
  attrs = list(df.columns)
  schema = Schema([])
  row = None
  if df.shape[0]:
    row = df.iloc[0].to_dict()

  for attr in attrs:
    typ = "str"
    if row:
      typ = guess_type(row[attr])
    schema.attrs.append(Attr(attr, typ))
  return schema



class Database(object):
  _db = None

  """
  Manages all tables registered in the database
  """
  def __init__(self, mode=Mode.ROW):
    self.registry = {}
    self.id2table = {}
    self._df_registry = {}
    self.function_registry = {}
    self.table_function_registry = {}
    self._mode = mode
    self.setup()

  @staticmethod
  def db(mode=Mode.ROW):
    if not Database._db:
      Database._db = Database(mode)
    return Database._db

  def setup(self):
    """
    Walks all CSV files in the current directory and registers
    them in the database
    """
    for root, dirs, files in os.walk("."):
      for fname in files:
        if self._mode == Mode.ROW:
          if fname.lower().endswith(".csv"):
            self.register_file_by_path(os.path.join(root, fname))
        else:
          if fname.lower().endswith(".tbl"):
            self.register_file_by_path(os.path.join(root, fname))

  def register_file_by_path(self, path):
    root, fname = os.path.split(path)
    tablename, _ = os.path.splitext(fname)
    fpath = os.path.join(root, fname)
    loaded = False
    exception = None
    for sep in [',', '|', '\t']:
      df = None
      try:
        with openfile(fpath) as f:
          df = pandas.read_csv(f, sep=sep)
      except Exception as e:
        exception = e

      if df is not None:
        self.register_dataframe(tablename, df)
        loaded = True
        break

    if not loaded:
      print("Failed to read data file %s" % (fpath))
      print(exception)


  def register_table(self, tablename, schema, table):
    self.registry[tablename] = table
    self.id2table[table.id] = table

  # TODO(ic2389): Register column-oriented dataframe
  def register_dataframe(self, tablename, df):
    self._df_registry[tablename] = df
    schema = infer_schema_from_df(df)

    if self._mode == Mode.ROW:
      rows = list(df.T.to_dict().values())
      rows = [[row[attr.aname] for attr in schema] for row in rows]
      table = InMemoryTable(schema, rows)
      self.register_table(tablename, schema, table)
    elif self._mode == Mode.COLUMN_ALL:
      print("[setup] table {tablename} with schema:".format(tablename=tablename), schema)
      columns = [df[df_colname].values.tolist() for df_colname in df]
      table = InMemoryColumnTable(schema, columns)
      self.register_table(tablename, schema, table)
    elif self._mode == Mode.COLUMN_SELECT:
      print("COLUMN_SELECT MODE IS NOT IMPLEMENTED")
      pass

  @property
  def tablenames(self):
    return list(self.registry.keys())

  @property
  def mode(self):
    return self._mode

  def schema(self, tablename):
    return self[tablename].schema

  def table_by_id(self, id):
    return self.id2table.get(id, None)

  def __contains__(self, tablename):
    return tablename in self.registry

  def __getitem__(self, tablename):
    return self.registry.get(tablename, None)

