import csv
import datetime
from dateutil.relativedelta import relativedelta
import os
import psycopg2
import time
from  queries_profile import run_script, run_queries, run_precomputations
from pathlib import Path
from itertools import cycle
import argparse

query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"]

parser = argparse.ArgumentParser()
parser.add_argument('--scale_factor', type=str, help='Scale factor', required=True)
parser.add_argument('--test', action='store_true', help='Test execution: 1 query/batch', required=False)
parser.add_argument('--validate', action='store_true', help='Validation mode', required=False)
parser.add_argument('--pgtuning', action='store_true', help='Paramgen tuning execution: 100 queries/batch', required=False)
parser.add_argument('--local', action='store_true', help='Local run (outside of a container)', required=False)
parser.add_argument('--data_dir', type=str, help='Directory with the initial_snapshot, insert, and delete directories', required=False)
parser.add_argument('--param_dir', type=str, help='Directory with the initial_snapshot, insert, and delete directories')
parser.add_argument('--queries', action='store_true', help='Only run queries', required=False)
parser.add_argument('--container_name', type=str, help='Name or ID of the container to monitor', required=True)

args = parser.parse_args()
sf = args.scale_factor
test = args.test
pgtuning = args.pgtuning
local = args.local
data_dir = args.data_dir
queries_only = args.queries
validate = args.validate
container_name = args.container_name

if args.param_dir is not None:
    param_dir = args.param_dir
else:
    param_dir = f'../parameters/parameters-sf{sf}'

parameter_csvs = {}
for query_variant in query_variants:
    # wrap parameters into infinite loop iterator
    parameter_csvs[query_variant] = cycle(csv.DictReader(open(f'{param_dir}/bi-{query_variant}.csv'), delimiter='|'))

output = Path(f"output/output-sf{sf}")
output.mkdir(parents=True, exist_ok=True)
open(f"output/output-sf{sf}/results.csv", "w").close()
open(f"output/output-sf{sf}/timings.csv", "w").close()

timings_file = open(f"output/output-sf{sf}/timings.csv", "a")
timings_file.write(f"tool|sf|day|batch_type|q|parameters|time\n")
results_file = open(f"output/output-sf{sf}/results.csv", "a")

pg_con = psycopg2.connect(host="localhost", user="postgres", password="zytzyt888999", port=5432)
pg_con.autocommit = True
cur = pg_con.cursor()

network_start_date = datetime.date(2012, 11, 29)
network_end_date = datetime.date(2013, 1, 1)
test_end_date = datetime.date(2012, 12, 2)
batch_size = relativedelta(days=1)
batch_date = network_start_date

benchmark_start = time.time()

if queries_only:
    run_precomputations(query_variants, pg_con, cur, batch_date, "power", sf, timings_file,container_name)
    run_queries(query_variants, parameter_csvs, pg_con, sf, test, pgtuning, batch_date, "power", timings_file, results_file,container_name)

cur.close()
pg_con.close()

benchmark_end = time.time()
benchmark_duration = benchmark_end - benchmark_start
benchmark_file = open(f"output/output-sf{sf}/benchmark.csv", "w")
benchmark_file.write(f"time\n")
benchmark_file.write(f"{benchmark_duration:.6f}\n")
benchmark_file.close()

timings_file.close()
results_file.close()

