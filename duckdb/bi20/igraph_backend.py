"""
bi20/igraph_backend.py
BI-20 accelerated path search: DuckDB Python API + igraph shortest paths.

The non-iterative part (edge extraction and source/destination filtering) stays in SQL,
while shortest-path computation is moved from recursive SQL to igraph.
"""

import json
import subprocess
import time
from pathlib import Path

import duckdb
import igraph as ig
import numpy as np

from db_config import db_file, format_value_duckdb
try:
    from bi20.shared_sql import (
        DSTS_SQL,
        EDGE_SELECT_SQL_FULL,
        EDGE_SELECT_SQL_PRECOMPUTED,
        SRCS_SQL,
    )
except ImportError:
    from shared_sql import DSTS_SQL, EDGE_SELECT_SQL_FULL, EDGE_SELECT_SQL_PRECOMPUTED, SRCS_SQL


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
        "graph_build": 0.0,
        "dijkstra": 0.0,
    }


def _print_phase_timings(timings, n_iters, out_csv_path=None):
    labels = {
        "duckdb_sql": "DuckDB SQL     ",
        "arrow_to_numpy": "Arrow->numpy   ",
        "graph_build": "Graph build    ",
        "dijkstra": "Shortest paths ",
    }
    total = sum(timings.values())
    print("\n[BI-20 phase timings / igraph]  (avg over {} iters)".format(n_iters))
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
    return "PathQ20(src, dst, w) as" in query_spec


def _solve_bi20(arrow_edges, src_arrow, dst_arrow, timings):
    if src_arrow.num_rows == 0 or dst_arrow.num_rows == 0:
        return []
    if arrow_edges.num_rows == 0:
        return []

    _t = time.perf_counter()
    src_edge = arrow_edges.column("src").to_numpy()
    dst_edge = arrow_edges.column("dst").to_numpy()
    w_edge = arrow_edges.column("weight").to_numpy()
    src_nodes = src_arrow.column("id").to_numpy()
    dst_nodes = dst_arrow.column("id").to_numpy()
    timings["arrow_to_numpy"] += time.perf_counter() - _t

    _t = time.perf_counter()
    all_nodes = np.unique(np.concatenate([src_edge, dst_edge, src_nodes, dst_nodes]))
    src_idx = np.searchsorted(all_nodes, src_edge)
    dst_idx = np.searchsorted(all_nodes, dst_edge)

    source_idx = np.searchsorted(all_nodes, src_nodes)
    if len(source_idx) == 0 or source_idx[0] >= len(all_nodes) or all_nodes[source_idx[0]] != src_nodes[0]:
        timings["graph_build"] += time.perf_counter() - _t
        return []
    dst_city_idx = np.searchsorted(all_nodes, dst_nodes)

    g = ig.Graph(n=len(all_nodes), directed=False)
    g.add_edges(list(zip(src_idx.tolist(), dst_idx.tolist())))
    g.es["weight"] = w_edge.tolist()
    timings["graph_build"] += time.perf_counter() - _t

    _t = time.perf_counter()
    dist = g.distances(
        source=int(source_idx[0]),
        target=dst_city_idx.tolist(),
        weights="weight",
    )[0]
    timings["dijkstra"] += time.perf_counter() - _t

    finite = []
    for i, t in enumerate(dst_nodes.tolist()):
        d = dist[i]
        if d != float("inf"):
            finite.append((int(t), float(d)))

    if not finite:
        return []

    best_w = min(x[1] for x in finite)
    best_rows = [x for x in finite if x[1] == best_w]
    best_rows.sort(key=lambda x: x[0])
    best_rows = best_rows[:20]

    normalized = []
    for t, w in best_rows:
        if float(w).is_integer():
            normalized.append({"t": t, "w": int(w)})
        else:
            normalized.append({"t": t, "w": float(w)})
    return normalized


def run_query_20(query_variant, query_spec, query_parameters, perf_file, phase_timings_dir=None):
    """BI-20 entry point with igraph shortest path computation."""
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

    perf_path = Path(perf_file)
    # 从 perf_file 中提取相对于 perf_base_dir 的部分（最后2层：query_id/parameters-i.csv）
    perf_parts = perf_path.parts
    if len(perf_parts) >= 2:
        rel_subpath = '/'.join(perf_parts[-2:])  # bi-20a/parameters-1.csv
    else:
        rel_subpath = str(perf_path)
    
    if phase_timings_dir is None:
        phase_out_path = str(Path('output_orig', 'phase_timings', rel_subpath))
    else:
        phase_out_path = str(Path(phase_timings_dir) / rel_subpath)

    start = time.time()
    try:
        for _ in range(10):
            _t = time.perf_counter()
            arrow_edges = con.execute(edge_sql).fetch_arrow_table()
            src_arrow = con.execute(SRCS_SQL).fetch_arrow_table()
            dst_arrow = con.execute(DSTS_SQL).fetch_arrow_table()
            timings["duckdb_sql"] += time.perf_counter() - _t

            result_rows = _solve_bi20(arrow_edges, src_arrow, dst_arrow, timings)
            last_result = json.dumps(result_rows)
    finally:
        con.close()

    duration = (time.time() - start) / 10
    perf.kill()

    _print_phase_timings(timings, 10, out_csv_path=phase_out_path)
    return last_result, duration
