"""
bi19/cached_cpp_backend.py
BI-19 cached graph backend (cpp + igraph):
  - Build graph in C++ once per edge SQL mode
  - Reuse C++ cached graph across repeated query iterations
  - Keep Python side focused on SQL extraction and timings
"""

import json
import subprocess
import time
from pathlib import Path

import duckdb

from db_config import db_file, format_value_duckdb
try:
    from bi19.shared_sql import (
        DSTS_SQL,
        EDGE_SELECT_SQL_FULL,
        EDGE_SELECT_SQL_PRECOMPUTED,
        SRCS_SQL,
    )
except ImportError:
    from shared_sql import DSTS_SQL, EDGE_SELECT_SQL_FULL, EDGE_SELECT_SQL_PRECOMPUTED, SRCS_SQL

try:
    from . import bi19_igraph_cached_cpp
except ImportError:
    import bi19_igraph_cached_cpp


def _open_connection(set_stmts):
    con = duckdb.connect(db_file, read_only=True)
    con.execute("SET GLOBAL TimeZone = 'Etc/UTC'")
    for stmt in set_stmts:
        con.execute(stmt)
    return con


def _make_timings():
    return {
        "duckdb_sql": 0.0,
        "cache_build": 0.0,
        "arrow_to_numpy": 0.0,
        "cpp_dijkstra": 0.0,
    }


def _print_phase_timings(timings, n_iters, out_csv_path=None):
    labels = {
        "duckdb_sql": "DuckDB SQL     ",
        "cache_build": "Cache build    ",
        "arrow_to_numpy": "Arrow->numpy   ",
        "cpp_dijkstra": "CPP shortest   ",
    }
    total = sum(timings.values())
    print("\n[BI-19 phase timings / cached-cpp-igraph]  (avg over {} iters)".format(n_iters))
    print(f"  {'Phase':<20} {'avg(ms)':>10} {'total(ms)':>11} {'ratio':>8}")
    print("  " + "-" * 52)
    for key, label in labels.items():
        t = timings[key]
        avg_ms = t / n_iters * 1000
        tot_ms = t * 1000
        ratio = t / total * 100 if total > 0 else 0.0
        print(f"  {label:<20} {avg_ms:>10.3f} {tot_ms:>11.3f} {ratio:>7.1f}%")
    print("  " + "-" * 52)
    tot_avg_ms = total / n_iters * 1000
    print(f"  {'TOTAL':<20} {tot_avg_ms:>10.3f} {total*1000:>11.3f} {'100.0':>7}%")
    print()

    if out_csv_path is not None:
        Path(out_csv_path).parent.mkdir(parents=True, exist_ok=True)
        with open(out_csv_path, "w") as f:
            f.write("phase,avg_ms,total_ms,ratio_pct\n")
            for key in labels:
                t = timings[key]
                avg_ms = t / n_iters * 1000
                tot_ms = t * 1000
                ratio = t / total * 100 if total > 0 else 0.0
                f.write(f"{key},{avg_ms:.6f},{tot_ms:.6f},{ratio:.4f}\n")
            f.write(f"TOTAL,{tot_avg_ms:.6f},{total*1000:.6f},100.0000\n")


def _is_full_mode_query(query_spec):
    return "PathQ19(src, dst, w) as" in query_spec


def _extract_arrow(con, sql, timings):
    _t = time.perf_counter()
    table = con.execute(sql).fetch_arrow_table()
    timings["duckdb_sql"] += time.perf_counter() - _t
    return table


def run_query_19(query_variant, query_spec, query_parameters, perf_file):
    """BI-19 entry point with C++ cached igraph graph."""
    set_stmts = []
    for k, v in query_parameters.items():
        param_name, param_type = k.split(":")
        set_stmts.append(f"SET variable {param_name} = {format_value_duckdb(v, param_type)}")

    edge_sql = EDGE_SELECT_SQL_FULL if _is_full_mode_query(query_spec) else EDGE_SELECT_SQL_PRECOMPUTED
    cache_key = f"{db_file}|{edge_sql}"

    perf = subprocess.Popen(
        ["python3", "/work/machine_performance_indicators/monitor_system_perf.py", perf_file]
    )

    last_result = json.dumps([])
    timings = _make_timings()
    con = _open_connection(set_stmts)

    perf_parts = Path(perf_file).parts
    phase_out_path = str(Path("output_orig", "phase_timings", *perf_parts[1:]))

    try:
        edge_tbl = _extract_arrow(con, edge_sql, timings)

        _t = time.perf_counter()
        edge_src = edge_tbl.column("src").to_numpy()
        edge_dst = edge_tbl.column("dst").to_numpy()
        edge_w = edge_tbl.column("weight").to_numpy()
        timings["arrow_to_numpy"] += time.perf_counter() - _t

        _t = time.perf_counter()
        bi19_igraph_cached_cpp.build_graph(edge_src, edge_dst, edge_w, cache_key)
        timings["cache_build"] += time.perf_counter() - _t

        start = time.time()
        for _ in range(10):
            src_tbl = _extract_arrow(con, SRCS_SQL, timings)
            dst_tbl = _extract_arrow(con, DSTS_SQL, timings)

            _t = time.perf_counter()
            src_nodes = src_tbl.column("id").to_numpy()
            dst_nodes = dst_tbl.column("id").to_numpy()
            timings["arrow_to_numpy"] += time.perf_counter() - _t

            _t = time.perf_counter()
            result_rows = bi19_igraph_cached_cpp.solve_cached(src_nodes, dst_nodes)
            timings["cpp_dijkstra"] += time.perf_counter() - _t

            last_result = json.dumps(result_rows)

        duration = (time.time() - start) / 10
    finally:
        con.close()
        perf.kill()

    _print_phase_timings(timings, 10, out_csv_path=phase_out_path)
    return last_result, duration
