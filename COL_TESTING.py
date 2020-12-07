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

test_q = "SELECT * FROM data"

def setup():
  mode = Mode.COLUMN_ALL # either Mode.ROW or Mode.COLUMN_ALL or Mode.COLUMN_SELECT
  print("[setup] db in mode...", mode)
  db = Database.db(mode)

  print("[setup] {num} tables...OK".format(num=len(db.tablenames)), db.tablenames)
  print("[setup]...OK")
  return db

def run_plan(plan):
  databass_rows = list()
  for row in plan:
    vals = []
    for v in row:
      if isinstance(v, str):
        vals.append(v)
      else:
        vals.append(float(v))
    databass_rows.append(vals)
  return databass_rows

def run_q(opt: Optimizer, q: str):
  plan = opt(parse(q).to_plan())
  return run_plan(plan)

def main():
  db = setup()
  opt = Optimizer(db)
  print(run_q(opt, test_q))


if __name__ == "__main__":
  main()