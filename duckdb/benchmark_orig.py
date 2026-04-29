import csv
import datetime
from dateutil.relativedelta import relativedelta
import os
from pathlib import Path
from itertools import cycle
import argparse
import time
import json
from queries_orig import run_queries, run_precomputations

query_variants = ["15a", "15b", ]#"1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9",
                #   "10a", "10b", "11", "12", "13", "14a", "14b", 
                #   "16a", "16b", "17", "18", ]"19a", "19b", "20a", "20b"

# python3 benchmark_orig.py --scale_factor 1 --queries --test --path_mode full --output_dir output_orig/exp_sf1_full_test_run01

parser = argparse.ArgumentParser()
parser.add_argument('--scale_factor', type=str, help='Scale factor', required=True)
parser.add_argument('--test', action='store_true', help='Test execution: 1 query/batch', required=False)
parser.add_argument('--pgtuning', action='store_true', help='Paramgen tuning execution: 100 queries/batch', required=False)
parser.add_argument('--param_dir', type=str, help='Directory with the initial_snapshot, insert, and delete directories')
parser.add_argument('--queries', action='store_true', help='Only run queries', required=False)
parser.add_argument('--path_mode', choices=['precompute', 'full'], default='full',
                    help='BI-19/20 path strategy: precompute uses PathQ19/PathQ20 tables, full computes paths inside query')
parser.add_argument('--output_dir', type=str, default=None,
                help='Output directory for results/timings/benchmark files. '
                    'Default: output_orig/output-sf{scale_factor}')
# parser.add_argument('--container_name', type=str, required=True)
args = parser.parse_args()

sf = args.scale_factor
test = args.test
pgtuning = args.pgtuning
queries_only = args.queries
use_precomputed_paths = args.path_mode == 'precompute'
# container_name = args.container_name
if args.param_dir is not None:
    param_dir = args.param_dir
else:
    param_dir = f'../parameters/parameters-sf{sf}'

if args.output_dir is not None:
    output = Path(args.output_dir)
else:
    output = Path(f"output_orig/output-sf{sf}")

phase_timings_dir = output / "phase_timings"
perf_base_dir = output / "system_performance"

parameter_csvs = {}
for query_variant in query_variants:
    # wrap parameters into infinite loop iterator
    parameter_csvs[query_variant] = cycle(csv.DictReader(open(f'{param_dir}/bi-{query_variant}.csv'), delimiter='|'))

output.mkdir(parents=True, exist_ok=True)
open(output / "results.csv", "w").close()
open(output / "timings.csv", "w").close()
timings_file = open(output / "timings.csv", "a")
timings_file.write("tool|sf|day|batch_type|q|parameters|time\n")
results_file = open(output / "results.csv", "a")

# batch_id = "2025-08-11"  # 示例日期，可根据需要调整
batch_type = "power"

network_start_date = datetime.date(2012, 11, 29)
network_end_date = datetime.date(2013, 1, 1)
test_end_date = datetime.date(2012, 12, 2)
batch_size = relativedelta(days=1)
batch_date = network_start_date

benchmark_start = time.time()

if queries_only:
    run_precomputations(
        query_variants, batch_date, batch_type, sf, timings_file,
        use_precomputed_paths=use_precomputed_paths,
        perf_base_dir=str(perf_base_dir)
    )
    run_queries(query_variants, parameter_csvs, sf, test, pgtuning, batch_date, batch_type, timings_file, results_file,
                use_precomputed_paths=use_precomputed_paths,
                phase_timings_dir=str(phase_timings_dir),
                perf_base_dir=str(perf_base_dir))


benchmark_end = time.time()
benchmark_duration = benchmark_end - benchmark_start

with open(output / "benchmark.csv", "w") as bf:
    bf.write("time\n")
    bf.write(f"{benchmark_duration:.6f}\n")

timings_file.close()
results_file.close()

