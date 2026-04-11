"""
bi19/cpp_backend.py
BI-19 主加速入口：DuckDB Python API + C++ shortest paths

逻辑保持与原 igraph 版本一致：
  1. DuckDB 执行非迭代 SQL，导出 edge table / src nodes / dst nodes
  2. C++ 扩展负责构图 + 多源多目标最短路径
  3. Python 只做参数绑定、计时和 JSON 序列化
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
    from . import bi19_dijkstra_cpp
    _BACKEND = "cpp"
except ImportError:
    try:
        import bi19_dijkstra_cpp
        _BACKEND = "cpp"
    except ImportError as exc:
        raise ImportError("bi19_dijkstra_cpp is not available; run bi19/setup.py build_ext --inplace") from exc


def _open_connection(set_stmts):
    con = duckdb.connect(db_file, read_only=True)
    con.execute("SET GLOBAL TimeZone = 'Etc/UTC'")
    for stmt in set_stmts:
        con.execute(stmt)
    return con


def _make_timings():
    return {
        "duckdb_sql": 0.0,
        "arrow_to_numpy": 0.0,
        "cpp_search": 0.0,
    }


def _print_phase_timings(timings, n_iters, out_csv_path=None):
    labels = {
        "duckdb_sql": "DuckDB SQL     ",
        "arrow_to_numpy": "Arrow->numpy   ",
        "cpp_search": "C++ shortest   ",
    }
    total = sum(timings.values())
    print("\n[BI-19 phase timings / cpp]  (avg over {} iters)".format(n_iters))
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


def _extract_nodes(con, sql, timings):
    _t = time.perf_counter()
    table = con.execute(sql).fetch_arrow_table()
    timings["duckdb_sql"] += time.perf_counter() - _t
    return table


def _solve_bi19_cpp(arrow_edges, src_arrow, dst_arrow, timings):
    if src_arrow.num_rows == 0 or dst_arrow.num_rows == 0 or arrow_edges.num_rows == 0:
        return []

    _t = time.perf_counter()
    src_edge = arrow_edges.column("src").to_numpy()
    dst_edge = arrow_edges.column("dst").to_numpy()
    w_edge = arrow_edges.column("weight").to_numpy()
    src_nodes = src_arrow.column("id").to_numpy()
    dst_nodes = dst_arrow.column("id").to_numpy()
    timings["arrow_to_numpy"] += time.perf_counter() - _t

    _t = time.perf_counter()
    result = bi19_dijkstra_cpp.solve_bi19(src_edge, dst_edge, w_edge, src_nodes, dst_nodes)
    timings["cpp_search"] += time.perf_counter() - _t
    return result


def run_query_19(query_variant, query_spec, query_parameters, perf_file):
    """BI-19 entry point with C++ shortest-path computation."""
    set_stmts = []
    for k, v in query_parameters.items():
        param_name, param_type = k.split(":")
        set_stmts.append(f"SET variable {param_name} = {format_value_duckdb(v, param_type)}")

    edge_sql = EDGE_SELECT_SQL_FULL if _is_full_mode_query(query_spec) else EDGE_SELECT_SQL_PRECOMPUTED

    perf = subprocess.Popen(
        ["python3", "/work/machine_performance_indicators/monitor_system_perf.py", perf_file]
    )

    last_result = json.dumps([])
    timings = _make_timings()
    con = _open_connection(set_stmts)

    perf_parts = Path(perf_file).parts
    phase_out_path = str(Path("output_orig", "phase_timings", *perf_parts[1:]))

    start = time.time()
    try:
        for _ in range(10):
            arrow_edges = _extract_nodes(con, edge_sql, timings)
            src_arrow = _extract_nodes(con, SRCS_SQL, timings)
            dst_arrow = _extract_nodes(con, DSTS_SQL, timings)

            result_rows = _solve_bi19_cpp(arrow_edges, src_arrow, dst_arrow, timings)
            last_result = json.dumps(result_rows)
    finally:
        con.close()

    duration = (time.time() - start) / 10
    perf.kill()

    _print_phase_timings(timings, 10, out_csv_path=phase_out_path)
    return last_result, duration
