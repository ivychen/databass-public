# Standard imports
import sys

# Databass imports
from databass import *

simple_test = [
  "SELECT * FROM data",
  "SELECT a, b FROM data"
]

debug_queries = [
  "SELECT * FROM date LIMIT 10",
  "SELECT c_custkey, s_suppkey FROM customer, supplier WHERE customer.c_nation = supplier.s_nation"
]

experiment_one = [
  "SELECT * from lineorder LIMIT 1",
  "SELECT custkey, suppkey FROM lineorder, supplier WHERE lineorder.suppkey = s_suppkey",
  "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date WHERE lo_orderdate = d_datekey AND d_year = 1993 AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25"
]

experiment_two = [
  "SELECT orderkey FROM lineorder",
  "SELECT category FROM part",
  "SELECT nation FROM customer WHERE nation = 'UNITED STATES'"
]

def run_query(db, qstr):
  plan = parse(qstr)
  plan = plan.to_plan()
  return run_plan(db, plan)

def run_plan(db, plan):
  databass_rows = list()
  plan = Optimizer(db)(plan)
  for row in plan:
    vals = []
    for v in row:
      if isinstance(v, str):
        vals.append(v)
      else:
        vals.append(float(v))
    databass_rows.append(vals)
  return databass_rows

def setup_row():
  print("=== ROW MODE: SETUP ===\n")
  mode = Mode.ROW # either Mode.ROW or Mode.COLUMN_ALL or Mode.COLUMN_SELECT
  print("[setup] db in mode...", mode)
  db = Database.db(mode)
  print("[setup] {num} tables...OK".format(num=len(db.tablenames)), db.tablenames)
  print("[setup] ...OK")

  print("\n=== ROW MODE: RUNNING QUERIES ===\n")

  for qstr in simple_test:
    print("[debug] running query: ", qstr)
    output = run_query(db, qstr)
    print(output)

  print("\n=== END ROW MODE ===\n")

def setup_col():
  print("=== RUNNING IN COL MODE ===\n")
  mode = Mode.COLUMN_ALL # either Mode.ROW or Mode.COLUMN_ALL or Mode.COLUMN_SELECT
  print("[setup] db in mode...", mode)
  db = Database.db(mode)
  print("[setup] {num} tables...OK".format(num=len(db.tablenames)), db.tablenames)
  
  print("[setup] ...OK")

  print("\n=== COL MODE: RUNNING QUERIES ===\n")

  for qstr in simple_test:
    print("[debug] running query: ", qstr)
    output = run_query(db, qstr)
    print(output)

  print("\n=== END COL MODE ===\n")

def main():
  setup_col()

if __name__ == "__main__":
  main()