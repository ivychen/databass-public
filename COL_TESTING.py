# Standard imports
import time

# Databass imports
from databass import *

simple_test = [
  "SELECT * FROM data",
  "SELECT a, b FROM data",
  "SELECT data.e FROM data GROUP BY data.e",
  "SELECT data.a, data4.a FROM data, data4 WHERE data.a = data4.a"
]

experiment_one = [
  "SELECT * from lineorder",
  "SELECT lo_custkey, lo_suppkey FROM lineorder, supplier WHERE lineorder.lo_suppkey = supplier.s_suppkey",
  "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date WHERE lo_orderdate = d_datekey AND d_year = 1993 AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25"
]

experiment_two = [
  "SELECT lo_orderkey FROM lineorder",
  "SELECT p_name, p_category FROM part LIMIT 20",
  "SELECT c_nation FROM customer WHERE nation = 'UNITED STATES'"
]

def run_query(db, qstr):
  plan = parse(qstr)
  plan = plan.to_plan()
  print("QUERY PLAN", plan.pretty_print())
  return run_plan(db, plan)

def run_plan(db, plan):
  databass_rows = list()
  # plan = Optimizer(db, SelingerOpt)(plan)
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
    print("[query]", qstr)
    start = time.time()
    output = run_query(db, qstr)
    # print("[output] ", output)
    print("[query] took %0.5f sec\n" % (time.time()-start))

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
    print("[query] ", qstr)
    start = time.time()
    output = run_query(db, qstr)
    print("[query] took %0.5f sec\n" % (time.time()-start))
    print("[output] ", output)

  print("\n=== END COL MODE ===\n")

def main():
  setup_col()

if __name__ == "__main__":
  main()