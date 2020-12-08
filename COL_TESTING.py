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
  "SELECT lo_custkey, lo_suppkey FROM lineorder, supplier WHERE lo_suppkey = s_suppkey",
  "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date WHERE lo_orderdate = d_datekey AND d_year = 1993 AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25"
]

experiment_two = [
  "SELECT lo_orderkey FROM lineorder",
  "SELECT p_name, p_category FROM part",
  "SELECT c_nation FROM customer WHERE c_nation = 'UNITED STATES'"
]

experiment_three = [
  "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date WHERE lo_orderdate = d_datekey AND d_yearmonth = 199401 AND lo_discount BETWEEN 4 AND 6 AND lo_quantity BETWEEN 26 AND 35",
  "SELECT sum(lo_revenue), d_year,p_brand FROM lineorder, date, part, supplier WHERE lo_orderdate = d_datekey AND lo_partkey = p_partkey AND lo_suppkey = s_suppkey AND p_category = 'MFGR#12' AND s_region = 'AMERICA' GROUP BY d_year, p_brand ORDER BY d_year, p_brand",
  "SELECT c_nation, s_nation, d_year, sum(lo_revenue) AS revenue FROM customer, lineorder, supplier, date WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_orderdate = d_datekey AND c_region = 'ASIA' AND s_region = 'ASIA' AND d_year >= 1992 AND d_year <= 1997 GROUP BY c_nation, s_nation, d_year ORDER BY d_year ASC, revenue DESC;",
  "SELECT d_year, c_nation, sum(lo_revenue - lo_supplycost) AS profit FROM date, customer, supplier, part, lineorder WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_partkey = p_partkey AND lo_orderdate = d_datekey AND c_region = 'AMERICA' AND s_region = 'AMERICA' AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2') GROUP BY d_year, c_nation ORDER BY d_year, c_nation;"
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
  setup_row()
  # setup_col()

if __name__ == "__main__":
  main()