"""
bi15_igraph.py
BI-15 加速实现：
  - DuckDB 负责非迭代部分（myForums 过滤 → mm 交互聚合 → path 边权计算），导出边表
  - igraph (C 实现) 负责迭代部分（Dijkstra 最短路径）
"""

import subprocess
import json
import time
from pathlib import Path
import igraph as ig

from db_config import duckdb_path, db_file, format_value_duckdb

# ---------------------------------------------------------------------------
# 非迭代 SQL：子图过滤 + 匹配 + 边权计算，输出 (src, dst, weight)
# ---------------------------------------------------------------------------
EDGE_SQL = """\
SET GLOBAL TimeZone = 'Etc/UTC';
{set_stmts}
with
myForums(id) as (
    select id from Forum f
    where f.creationDate between getvariable('startDate') and getvariable('endDate')
),
mm as (
    select least(msg.CreatorPersonId, reply.CreatorPersonId)    as src,
           greatest(msg.CreatorPersonId, reply.CreatorPersonId) as dst,
           sum(case when msg.ParentMessageId is null then 10 else 5 end) as w
    from undirected_Person_knows_Person pp, Message msg, Message reply
    where pp.person1id = msg.CreatorPersonId
      and pp.person2id = reply.CreatorPersonId
      and reply.ParentMessageId = msg.MessageId
      and exists (select * from myForums f where f.id = msg.containerforumid)
      and exists (select * from myForums f where f.id = reply.containerforumid)
    group by src, dst
)
select pp.person1id as src,
       pp.person2id as dst,
       10.0 / (coalesce(mm.w, 0) + 10) as weight
from undirected_Person_knows_Person pp
left join mm
       on least(pp.person1id, pp.person2id)    = mm.src
      and greatest(pp.person1id, pp.person2id) = mm.dst;
"""


def _make_timings():
    """返回各计算阶段计时累计器（单位：秒）。"""
    return {
        'duckdb_cli':   0.0,   # subprocess 调用 DuckDB CLI（SQL 执行 + JSON 序列化）
        'json_parse':   0.0,   # json.loads 反序列化
        'node_mapping': 0.0,   # Python set/dict 构建节点 ID 映射
        'graph_build':  0.0,   # igraph 图构建（add_edges + 边权赋值）
        'dijkstra':     0.0,   # igraph Dijkstra 求最短路径
    }


def _print_phase_timings(timings, n_iters, out_csv_path=None):
    """将各阶段耗时汇总打印到 stdout，并可选将结果写入 CSV 文件。"""
    labels = {
        'duckdb_cli':   'DuckDB CLI     ',
        'json_parse':   'JSON parse     ',
        'node_mapping': 'Node mapping   ',
        'graph_build':  'Graph build    ',
        'dijkstra':     'Dijkstra       ',
    }
    total = sum(timings.values())
    print("\n[BI-15 phase timings]  (avg over {} iters)".format(n_iters))
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

    # 写入 CSV 文件
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
    """调用 DuckDB CLI，执行非迭代 SQL，返回边列表 [{src, dst, weight}, ...]。"""
    sql = EDGE_SQL.format(set_stmts=set_stmts_str)

    # --- 阶段 0: DuckDB CLI 调用 ---
    _t = time.perf_counter()
    proc = subprocess.run(
        [duckdb_path, db_file, '-json', '-c', sql],
        capture_output=True, text=True
    )
    timings['duckdb_cli'] += time.perf_counter() - _t

    raw = proc.stdout.strip()
    if not raw:
        return []

    # --- 阶段 1: JSON 反序列化 ---
    _t = time.perf_counter()
    result = json.loads(raw)
    timings['json_parse'] += time.perf_counter() - _t
    return result


def _dijkstra(edges_data, person1Id, person2Id, timings):
    """在 edges_data 上构建 igraph 无向图，用 Dijkstra 求最短路径权重之和。"""
    # --- 阶段 2: 节点 ID 映射 ---
    _t = time.perf_counter()
    node_set = set()
    for e in edges_data:
        node_set.add(e['src'])
        node_set.add(e['dst'])
    id_map = {v: i for i, v in enumerate(sorted(node_set))}
    timings['node_mapping'] += time.perf_counter() - _t

    if person1Id not in id_map or person2Id not in id_map:
        return -1

    # --- 阶段 3: igraph 图构建 ---
    _t = time.perf_counter()
    g = ig.Graph(n=len(id_map), directed=False)
    g.add_edges([(id_map[e['src']], id_map[e['dst']]) for e in edges_data])
    g.es['weight'] = [e['weight'] for e in edges_data]
    timings['graph_build'] += time.perf_counter() - _t

    # --- 阶段 4: Dijkstra（igraph C 实现）---
    _t = time.perf_counter()
    dist = g.distances(
        source=id_map[person1Id],
        target=id_map[person2Id],
        weights='weight'
    )[0][0]
    timings['dijkstra'] += time.perf_counter() - _t

    return dist if dist != float('inf') else -1


def run_query_15(query_variant, query_parameters, perf_file):
    """
    BI-15 入口，接口与 run_query 返回值一致：(result_json_str, duration_seconds)。
    执行 10 次取平均耗时，与其他查询保持一致。
    """
    # 解析 person1Id / person2Id
    param_dict = {k.split(':')[0]: v for k, v in query_parameters.items()}
    person1Id = int(param_dict['person1Id'])
    person2Id = int(param_dict['person2Id'])

    # 构造 SET variable 语句串
    set_stmts = []
    for k, v in query_parameters.items():
        param_name, param_type = k.split(':')
        set_stmts.append(f"SET variable {param_name} = {format_value_duckdb(v, param_type)};")
    set_stmts_str = '\n'.join(set_stmts)

    # 性能监控
    perf = subprocess.Popen(
        ["python3", "/work/machine_performance_indicators/monitor_system_perf.py", perf_file]
    )

    last_result = json.dumps([{"coalesce(min(w), -1)": -1}])
    timings = _make_timings()
    n_valid = 0

    # 从 perf_file 派生阶段计时 CSV 输出路径
    # perf_file 示例: system_performance_orig/bi-15a/parameters-1.csv
    # 输出路径: output_orig/phase_timings/bi-15a/parameters-1.csv
    perf_parts = Path(perf_file).parts
    phase_out_path = str(Path('output_orig', 'phase_timings', *perf_parts[1:]))

    start = time.time()

    for _ in range(10):
        edges_data = _extract_edges(set_stmts_str, timings)   # DuckDB CLI + JSON 解析
        if not edges_data:
            last_result = json.dumps([{"coalesce(min(w), -1)": -1}])
            continue
        result_val = _dijkstra(edges_data, person1Id, person2Id, timings)  # igraph：最短路径
        last_result = json.dumps([{"coalesce(min(w), -1)": result_val}])
        n_valid += 1

    duration = (time.time() - start) / 10
    perf.kill()

    _print_phase_timings(timings, max(n_valid, 1), out_csv_path=phase_out_path)

    return last_result, duration
