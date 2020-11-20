class Column(object):
  """
  Represents a column in a columnar database.
  """
  def __init__(self):
    self.data = None

class ArrayColumn(Column):
  """
  Represents a column that stores data in arrays.
  """
  def __init__(self):
    super(ArrayColumn, self).__init__()
    self.data = []