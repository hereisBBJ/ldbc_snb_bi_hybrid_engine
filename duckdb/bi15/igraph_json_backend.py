"""
bi15/igraph_json_backend.py
BI-15 备用实现：DuckDB CLI (subprocess) + JSON 解析 + igraph Dijkstra
  - 最早版本，保留作对比基准（CLI 序列化开销最大）
"""

import subprocess
import json
import time
from pathlib import Path
import igraph as ig

from db_config import duckdb_path, db_file, format_value_duckdb
try:
    from bi15.shared_sql import EDGE_SELECT_SQL
except ImportError:
    from shared_sql import EDGE_SELECT_SQL

# ---------------------------------------------------------------------------
# 非迭代 SQL（嵌入 SET 语句后通过 CLI 执行）
# ---------------------------------------------------------------------------
EDGE_SQL = """\
SET GLOBAL TimeZone = 'Etc/UTC';
{set_stmts}
{edge_select_sql};
"""


def _make_timings():
    return {
        'duckdb_cli':   0.0,
        'json_parse':   0.0,
        'node_mapping': 0.0,
        'graph_build':  0.0,
        'dijkstra':     0.0,
    }


def _print_phase_timings(timings, n_iters, out_csv_path=None):
    labels = {
        'duckdb_cli':   'DuckDB CLI     ',
        'json_parse':   'JSON parse     ',
        'node_mapping': 'Node mapping   ',
        'graph_build':  'Graph build    ',
        'dijkstra':     'Dijkstra       ',
    }
    total = sum(timings.values())
    print("\n[BI-15 phase timings / igraph-json]  (avg over {} iters)".format(n_iters))
    print(f"  {'Phase':<20} {'avg(ms)':>10} {'total(ms)':>11} {'ratio':>8}")
    print("  " + "-" * 52)
    for key, label in labels.items():
        t = timings[key]
        avg_ms = t / n_iters * 1000
        tot_ms = t * 1000
        ratio  = t / total * 100 if total > 0 else 0.0
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


def _extract_edges(set_stmts_str, timings):
    sql = EDGE_SQL.format(set_stmts=set_stmts_str, edge_select_sql=EDGE_SELECT_SQL)

    _t = time.perf_counter()
    proc = subprocess.run(
        [duckdb_path, db_file, '-json', '-c', sql],
        capture_output=True, text=True
    )
    timings['duckdb_cli'] += time.perf_counter() - _t

    raw = proc.stdout.strip()
    if not raw:
        return []

    _t = time.perf_counter()
    result = json.loads(raw)
    timings['json_parse'] += time.perf_counter() - _t
    return result


def _dijkstra(edges_data, person1Id, person2Id, timings):
    _t = time.perf_counter()
    node_set = set()
    for e in edges_data:
        node_set.add(e['src'])
        node_set.add(e['dst'])
    id_map = {v: i for i, v in enumerate(sorted(node_set))}
    timings['node_mapping'] += time.perf_counter() - _t

    if person1Id not in id_map or person2Id not in id_map:
        return -1

    _t = time.perf_counter()
    g = ig.Graph(n=len(id_map), directed=False)
    g.add_edges([(id_map[e['src']], id_map[e['dst']]) for e in edges_data])
    g.es['weight'] = [e['weight'] for e in edges_data]
    timings['graph_build'] += time.perf_counter() - _t

    _t = time.perf_counter()
    dist = g.distances(
        source=id_map[person1Id],
        target=id_map[person2Id],
        weights='weight'
    )[0][0]
    timings['dijkstra'] += time.perf_counter() - _t

    return dist if dist != float('inf') else -1


def run_query_15(query_variant, query_parameters, perf_file, phase_timings_dir=None):
    """BI-15 入口（CLI+JSON+igraph 实现），接口与 pybind_backend.run_query_15 一致。"""
    param_dict = {k.split(':')[0]: v for k, v in query_parameters.items()}
    person1Id = int(param_dict['person1Id'])
    person2Id = int(param_dict['person2Id'])

    set_stmts = []
    for k, v in query_parameters.items():
        param_name, param_type = k.split(':')
        set_stmts.append(f"SET variable {param_name} = {format_value_duckdb(v, param_type)};")
    set_stmts_str = '\n'.join(set_stmts)

    perf = subprocess.Popen(
        ["python3", "/work/machine_performance_indicators/monitor_system_perf.py", perf_file]
    )

    last_result = json.dumps([{"coalesce(min(w), -1)": -1}])
    timings = _make_timings()
    n_valid = 0

    perf_path = Path(perf_file)
    # 从 perf_file 中提取相对于 perf_base_dir 的部分（最后2层：query_id/parameters-i.csv）
    perf_parts = perf_path.parts
    if len(perf_parts) >= 2:
        rel_subpath = '/'.join(perf_parts[-2:])  # bi-15a/parameters-1.csv
    else:
        rel_subpath = str(perf_path)
    
    if phase_timings_dir is None:
        phase_out_path = str(Path('output_orig', 'phase_timings', rel_subpath))
    else:
        phase_out_path = str(Path(phase_timings_dir) / rel_subpath)

    start = time.time()

    for _ in range(10):
        edges_data = _extract_edges(set_stmts_str, timings)
        if not edges_data:
            last_result = json.dumps([{"coalesce(min(w), -1)": -1}])
            continue
        result_val = _dijkstra(edges_data, person1Id, person2Id, timings)
        last_result = json.dumps([{"coalesce(min(w), -1)": result_val}])
        n_valid += 1

    duration = (time.time() - start) / 10
    perf.kill()

    _print_phase_timings(timings, max(n_valid, 1), out_csv_path=phase_out_path)
    return last_result, duration
