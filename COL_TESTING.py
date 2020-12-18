# Standard imports
import time
from resource import *

# Databass imports
from databass import *

queries_part = [
  "SELECT p_name FROM part",
  "SELECT p_name, p_category FROM part",
  "SELECT p_partkey,p_name,p_mfgr FROM part",
  "SELECT * FROM part",
]

queries_date = [
  "SELECT d_datekey FROM date",
  "SELECT d_datekey, d_date FROM date",
  "SELECT d_datekey, d_date,d_daynuminmonth,d_sellingseason FROM date",
  "SELECT d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,d_daynuminweek FROM date",
  "SELECT d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear,d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl FROM date",
  "SELECT * FROM date",
]

queries_customer = [
  "SELECT c_name FROM customer",
  "SELECT c_name, c_address FROM customer",
  "SELECT c_name, c_address, c_phone, c_mktsegment FROM customer",
  "SELECT * FROM customer",
]

queries_lineorder = [
  "SELECT lo_orderkey FROM lineorder",
  "SELECT lo_orderkey,lo_linenumber FROM lineorder",
  "SELECT lo_orderkey,lo_linenumber,lo_custkey,lo_partkey FROM lineorder",
  "SELECT lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,lo_shippriority FROM lineorder",
  "SELECT lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,lo_revenue,lo_supplycost,lo_tax,lo_commitdate FROM lineorder",
  "SELECT * FROM lineorder",
]

experiment_one = [
  "SELECT * from lineorder",
  "SELECT lo_custkey, lo_suppkey FROM lineorder, supplier WHERE lo_suppkey = s_suppkey",
  "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date WHERE lo_orderdate = d_datekey AND d_year = 1993 AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25"
]

experiment_two = [
  # "SELECT lo_orderkey FROM lineorder",
  # "SELECT p_name, p_category FROM part",
  "SELECT s_name, c_name FROM supplier, customer WHERE supplier.s_city == customer.c_city",
  # "SELECT c_nation FROM customer WHERE c_nation = 'UNITED STATES'"
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
  # print("QUERY PLAN", plan.pretty_print())
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

  for qstr in queries_lineorder:
    print("[query]", qstr)
    startMem = getrusage(RUSAGE_SELF).ru_maxrss
    start = time.time()
    output = run_query(db, qstr)
    # print("[output] ", output)
    print("[query time] took %0.5f sec\n" % (time.time()-start))
    print("[query memory] ru_maxrss diff %0.5f bytes" % (getrusage(RUSAGE_SELF).ru_maxrss-startMem))

  print("\n=== END ROW MODE ===\n")

def setup_col():
  print("=== RUNNING IN COL MODE ===\n")
  mode = Mode.COLUMN_ALL # either Mode.ROW or Mode.COLUMN_ALL or Mode.COLUMN_SELECT
  print("[setup] db in mode...", mode)
  db = Database.db(mode)
  print("[setup] {num} tables...OK".format(num=len(db.tablenames)), db.tablenames)
  
  print("[setup] ...OK")

  print("\n=== COL MODE: RUNNING QUERIES ===\n")

  for qstr in queries_lineorder:
    print("[query]", qstr)
    startMem = getrusage(RUSAGE_SELF).ru_maxrss
    start = time.time()
    output = run_query(db, qstr)
    # print("[output] ", output)
    print("[query time] took %0.5f sec\n" % (time.time()-start))
    print("[query memory] ru_maxrss diff %0.5f bytes" % (getrusage(RUSAGE_SELF).ru_maxrss-startMem))

  print("\n=== END COL MODE ===\n")

def main():
  # setup_row()
  setup_col()

if __name__ == "__main__":
  main()