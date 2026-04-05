"""
bi15/igraph_backend.py
BI-15 备用实现：DuckDB Python API (Arrow 零拷贝) + igraph Dijkstra
  - numpy 向量化构建节点映射，减少 Python 层遍历开销

此文件保留作对比基准；主入口 pybind_backend.py 在 C++ 扩展不可用时自动回退到
此文件的等效逻辑（内联在 _dijkstra_igraph 函数中）。
"""

import subprocess
import json
import time
from pathlib import Path
import duckdb
import numpy as np
import igraph as ig

from db_config import db_file, format_value_duckdb
try:
    from bi15.shared_sql import EDGE_SELECT_SQL
except ImportError:
    from shared_sql import EDGE_SELECT_SQL


def _open_connection(set_stmts):
    con = duckdb.connect(db_file, read_only=True)
    con.execute("SET GLOBAL TimeZone = 'Etc/UTC'")
    for stmt in set_stmts:
        con.execute(stmt)
    return con


def _make_timings():
    return {
        'duckdb_sql':    0.0,
        'arrow_to_numpy': 0.0,
        'node_mapping':  0.0,
        'graph_build':   0.0,
        'dijkstra':      0.0,
    }


def _print_phase_timings(timings, n_iters, out_csv_path=None):
    labels = {
        'duckdb_sql':     'DuckDB SQL     ',
        'arrow_to_numpy': 'Arrow→numpy    ',
        'node_mapping':   'Node mapping   ',
        'graph_build':    'Graph build    ',
        'dijkstra':       'Dijkstra       ',
    }
    total = sum(timings.values())
    print("\n[BI-15 phase timings / igraph]  (avg over {} iters)".format(n_iters))
    print(f"  {'Phase':<20} {'avg(ms)':>10} {'total(ms)':>11} {'ratio':>8}")
    print("  " + "-" * 52)
    for key, label in labels.items():
        t = timings[key]
        avg_ms  = t / n_iters * 1000
        tot_ms  = t * 1000
        ratio   = t / total * 100 if total > 0 else 0.0
        print(f"  {label:<20} {avg_ms:>10.3f} {tot_ms:>11.3f} {ratio:>7.1f}%")
    print("  " + "-" * 52)
    tot_avg_ms = total / n_iters * 1000
    print(f"  {'TOTAL':<20} {tot_avg_ms:>10.3f} {total*1000:>11.3f} {'100.0':>7}%")
    print()

    if out_csv_path is not None:
        Path(out_csv_path).parent.mkdir(parents=True, exist_ok=True)
        with open(out_csv_path, 'w') as f:
            f.write('phase,avg_ms,total_ms,ratio_pct\n')
            for key in labels:
                t = timings[key]
                avg_ms = t / n_iters * 1000
                tot_ms = t * 1000
                ratio  = t / total * 100 if total > 0 else 0.0
                f.write(f'{key},{avg_ms:.6f},{tot_ms:.6f},{ratio:.4f}\n')
            f.write(f'TOTAL,{tot_avg_ms:.6f},{total*1000:.6f},100.0000\n')


def _dijkstra_arrow(arrow_table, person1Id, person2Id, timings):
    if arrow_table.num_rows == 0:
        return -1

    _t = time.perf_counter()
    src_arr    = arrow_table.column('src').to_numpy()
    dst_arr    = arrow_table.column('dst').to_numpy()
    weight_arr = arrow_table.column('weight').to_numpy()
    timings['arrow_to_numpy'] += time.perf_counter() - _t

    _t = time.perf_counter()
    all_nodes = np.unique(np.concatenate([src_arr, dst_arr]))
    p1_idx = np.searchsorted(all_nodes, person1Id)
    p2_idx = np.searchsorted(all_nodes, person2Id)
    if p1_idx >= len(all_nodes) or all_nodes[p1_idx] != person1Id:
        timings['node_mapping'] += time.perf_counter() - _t
        return -1
    if p2_idx >= len(all_nodes) or all_nodes[p2_idx] != person2Id:
        timings['node_mapping'] += time.perf_counter() - _t
        return -1
    src_idx = np.searchsorted(all_nodes, src_arr)
    dst_idx = np.searchsorted(all_nodes, dst_arr)
    timings['node_mapping'] += time.perf_counter() - _t

    _t = time.perf_counter()
    g = ig.Graph(n=len(all_nodes), directed=False)
    g.add_edges(list(zip(src_idx.tolist(), dst_idx.tolist())))
    g.es['weight'] = weight_arr.tolist()
    timings['graph_build'] += time.perf_counter() - _t

    _t = time.perf_counter()
    dist = g.distances(
        source=int(p1_idx),
        target=int(p2_idx),
        weights='weight'
    )[0][0]
    timings['dijkstra'] += time.perf_counter() - _t

    return dist if dist != float('inf') else -1


def run_query_15(query_variant, query_parameters, perf_file):
    """BI-15 入口（igraph 实现），接口与 pybind_backend.run_query_15 一致。"""
    param_dict = {k.split(':')[0]: v for k, v in query_parameters.items()}
    person1Id = int(param_dict['person1Id'])
    person2Id = int(param_dict['person2Id'])

    set_stmts = []
    for k, v in query_parameters.items():
        param_name, param_type = k.split(':')
        set_stmts.append(f"SET variable {param_name} = {format_value_duckdb(v, param_type)}")

    perf = subprocess.Popen(
        ["python3", "/work/machine_performance_indicators/monitor_system_perf.py", perf_file]
    )

    last_result = json.dumps([{"coalesce(min(w), -1)": -1}])
    con = _open_connection(set_stmts)
    timings = _make_timings()
    n_valid = 0

    perf_parts = Path(perf_file).parts
    phase_out_path = str(Path('output_orig', 'phase_timings', *perf_parts[1:]))

    start = time.time()
    try:
        for _ in range(10):
            _t = time.perf_counter()
            arrow_table = con.execute(EDGE_SELECT_SQL).fetch_arrow_table()
            timings['duckdb_sql'] += time.perf_counter() - _t

            if arrow_table.num_rows == 0:
                last_result = json.dumps([{"coalesce(min(w), -1)": -1}])
                continue

            result_val = _dijkstra_arrow(arrow_table, person1Id, person2Id, timings)
            last_result = json.dumps([{"coalesce(min(w), -1)": result_val}])
            n_valid += 1
    finally:
        con.close()

    duration = (time.time() - start) / 10
    perf.kill()

    _print_phase_timings(timings, max(n_valid, 1), out_csv_path=phase_out_path)
    return last_result, duration
