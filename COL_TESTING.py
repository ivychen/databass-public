# Standard imports
import sys

# Databass imports
from databass import *

experiment_one = [
  "SELECT * from lineorder",
  "SELECT custkey, suppkey FROM lineorder, supplier WHERE lineorder.SUPPKEY = s_suppkey",
  "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date WHERE lo_orderdate = d_datekey AND d_year = 1993 AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25"
]

experiemnt_two = [
  "SELECT orderkey FROM lineorder",
  "SELECT category FROM part",
  "SELECT nation FROM customer WHERE nation = 'UNITED STATES'"
]

def setup():
  mode = Mode.COLUMN_ALL # either Mode.ROW or Mode.COLUMN_ALL or Mode.COLUMN_SELECT
  print("[setup] db in mode...", mode)
  db = Database.db(mode)

  print("[setup] {num} tables...OK".format(num=len(db.tablenames)), db.tablenames)

  print("[setup]...OK")

def main():
  setup()

if __name__ == "__main__":
  main()