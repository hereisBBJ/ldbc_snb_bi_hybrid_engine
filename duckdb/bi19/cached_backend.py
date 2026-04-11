"""
bi19/cached_backend.py
BI-19 cached-graph backend:
  - DuckDB SQL is still used to extract the edge table / source city nodes / target city nodes
  - The expensive graph construction step is cached per edge_sql variant
  - Subsequent queries reuse the igraph graph and node index mapping, and only rerun shortest paths

This module is intentionally separate from cpp_backend.py so you can compare:
  1) SQL + graph rebuild + shortest paths
  2) SQL + cached graph + shortest paths
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
    from bi19.shared_sql import (
        DSTS_SQL,
        EDGE_SELECT_SQL_FULL,
        EDGE_SELECT_SQL_PRECOMPUTED,
        SRCS_SQL,
    )
except ImportError:
    from shared_sql import DSTS_SQL, EDGE_SELECT_SQL_FULL, EDGE_SELECT_SQL_PRECOMPUTED, SRCS_SQL


class _GraphCache:
    """Cache one igraph graph per SQL edge extractor."""

    def __init__(self):
        self.edge_sql = None
        self.graph = None
        self.node_to_idx = {}
        self.edge_count = 0

    def rebuild_if_needed(self, edge_sql, con):
        if self.edge_sql == edge_sql and self.graph is not None:
            return False

        _t = time.perf_counter()
        edges_tbl = con.execute(edge_sql).fetch_arrow_table()
        src_edge = edges_tbl.column("src").to_numpy()
        dst_edge = edges_tbl.column("dst").to_numpy()
        w_edge = edges_tbl.column("weight").to_numpy()

        all_nodes = np.unique(np.concatenate([src_edge, dst_edge]))
        node_to_idx = {int(node_id): int(index) for index, node_id in enumerate(all_nodes.tolist())}
        src_idx = np.array([node_to_idx[int(node_id)] for node_id in src_edge], dtype=np.int32)
        dst_idx = np.array([node_to_idx[int(node_id)] for node_id in dst_edge], dtype=np.int32)

        graph = ig.Graph(n=len(all_nodes), directed=False)
        graph.add_edges(list(zip(src_idx.tolist(), dst_idx.tolist())))
        graph.es["weight"] = w_edge.tolist()

        self.edge_sql = edge_sql
        self.graph = graph
        self.node_to_idx = node_to_idx
        self.edge_count = int(edges_tbl.num_rows)
        self._build_sec = time.perf_counter() - _t
        return True

    @property
    def build_sec(self):
        return getattr(self, "_build_sec", 0.0)


_CACHE = _GraphCache()


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
        "dijkstra": 0.0,
    }


def _print_phase_timings(timings, n_iters, out_csv_path=None):
    labels = {
        "duckdb_sql": "DuckDB SQL     ",
        "cache_build": "Cache build    ",
        "arrow_to_numpy": "Arrow->numpy   ",
        "dijkstra": "Shortest paths ",
    }
    total = sum(timings.values())
    print("\n[BI-19 phase timings / cached-igraph]  (avg over {} iters)".format(n_iters))
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


def _run_cached_shortest_paths(src_arrow, dst_arrow, timings):
    if src_arrow.num_rows == 0 or dst_arrow.num_rows == 0:
        return []
    if _CACHE.graph is None or not _CACHE.node_to_idx:
        return []

    _t = time.perf_counter()
    src_nodes = src_arrow.column("id").to_numpy()
    dst_nodes = dst_arrow.column("id").to_numpy()
    timings["arrow_to_numpy"] += time.perf_counter() - _t

    _t = time.perf_counter()
    missing_nodes = []
    for node_id in src_nodes.tolist():
        if int(node_id) not in _CACHE.node_to_idx:
            missing_nodes.append(int(node_id))
    for node_id in dst_nodes.tolist():
        if int(node_id) not in _CACHE.node_to_idx:
            missing_nodes.append(int(node_id))

    if missing_nodes:
        start_idx = _CACHE.graph.vcount()
        _CACHE.graph.add_vertices(len(missing_nodes))
        for offset, node_id in enumerate(missing_nodes):
            _CACHE.node_to_idx[node_id] = start_idx + offset

    src_idx = np.array([_CACHE.node_to_idx[int(node_id)] for node_id in src_nodes.tolist()], dtype=np.int32)
    dst_idx = np.array([_CACHE.node_to_idx[int(node_id)] for node_id in dst_nodes.tolist()], dtype=np.int32)

    dist_mat = _CACHE.graph.distances(
        source=src_idx.tolist(),
        target=dst_idx.tolist(),
        weights="weight",
    )
    timings["dijkstra"] += time.perf_counter() - _t

    finite = []
    for i, s in enumerate(src_nodes.tolist()):
        row = dist_mat[i]
        for j, t in enumerate(dst_nodes.tolist()):
            d = row[j]
            if d != float("inf"):
                finite.append((int(s), int(t), float(d)))

    if not finite:
        return []

    best_w = min(x[2] for x in finite)
    best_rows = [x for x in finite if x[2] == best_w]
    best_rows.sort(key=lambda x: (x[0], x[1]))
    return [{"f": f, "t": t, "w": w} for f, t, w in best_rows]


def run_query_19(query_variant, query_spec, query_parameters, perf_file):
    """BI-19 entry point with cached igraph graph."""
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

    try:
        _t = time.perf_counter()
        _CACHE.rebuild_if_needed(edge_sql, con)
        timings["cache_build"] += time.perf_counter() - _t

        start = time.time()
        for _ in range(10):
            src_arrow = _extract_nodes(con, SRCS_SQL, timings)
            dst_arrow = _extract_nodes(con, DSTS_SQL, timings)
            result_rows = _run_cached_shortest_paths(src_arrow, dst_arrow, timings)
            last_result = json.dumps(result_rows)
        duration = (time.time() - start) / 10
    finally:
        con.close()
        perf.kill()

    _print_phase_timings(timings, 10, out_csv_path=phase_out_path)
    return last_result, duration
