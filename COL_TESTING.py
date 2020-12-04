# Standard imports
import sys

# Databass imports
from databass import *

simple_queries = [
  "SELECT * FROM tdata LIMIT 2"
]

def setup():
  mode = Mode.COLUMN_ALL # either Mode.ROW or Mode.COLUMN_ALL or Mode.COLUMN_SELECT
  print("setup db in mode...", mode)
  db = Database.db(mode)

  print("setup {num} tables...".format(num=len(db.tablenames)), db.tablenames)

  print("setup...OK")


def main():
  setup()

if __name__ == "__main__":
  main()