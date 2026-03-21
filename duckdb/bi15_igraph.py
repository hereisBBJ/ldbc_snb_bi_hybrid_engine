"""
bi15_igraph.py
BI-15 加速实现：
  - DuckDB Python API 负责非迭代部分（myForums 过滤 → mm 交互聚合 → path 边权计算），
    通过 Arrow 零拷贝导出边表
  - igraph (C 实现) 负责迭代部分（Dijkstra 最短路径）
  - numpy 向量化构建节点映射，减少 Python 层遍历开销
"""

import subprocess
import json
import time
from pathlib import Path
import duckdb
import numpy as np
import igraph as ig

from db_config import db_file, format_value_duckdb

# ---------------------------------------------------------------------------
# 非迭代 SQL：子图过滤 + 匹配 + 边权计算，输出 (src, dst, weight)
# ---------------------------------------------------------------------------
EDGE_SELECT_SQL = """\
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
      and greatest(pp.person1id, pp.person2id) = mm.dst
where pp.person1id < pp.person2id  -- 去重：每对节点只保留一条边，避免 igraph multigraph
"""


def _open_connection(set_stmts):
    """打开 DuckDB 连接（只读），执行 SET 语句，返回可复用的连接。"""
    con = duckdb.connect(db_file, read_only=True)
    con.execute("SET GLOBAL TimeZone = 'Etc/UTC'")
    for stmt in set_stmts:
        con.execute(stmt)
    return con


def _extract_edges_arrow(con):
    """通过 DuckDB Python API 执行 SELECT，以 Arrow Table 零拷贝返回 (src, dst, weight)。"""
    return con.execute(EDGE_SELECT_SQL).fetch_arrow_table()


def _make_timings():
    """返回各计算阶段计时累计器（单位：秒）。"""
    return {
        'duckdb_sql':    0.0,   # DuckDB 执行 SQL 并返回 Arrow Table
        'arrow_to_numpy': 0.0,  # Arrow 列 → numpy 数组
        'node_mapping':  0.0,   # np.unique / np.searchsorted 节点映射
        'graph_build':   0.0,   # igraph 图构建（add_edges + 边权赋值）
        'dijkstra':      0.0,   # igraph Dijkstra 求最短路径
    }


def _print_phase_timings(timings, n_iters, out_csv_path=None):
    """将各阶段耗时汇总打印到 stdout，并可选将结果写入 CSV 文件。"""
    labels = {
        'duckdb_sql':     'DuckDB SQL     ',
        'arrow_to_numpy': 'Arrow→numpy    ',
        'node_mapping':   'Node mapping   ',
        'graph_build':    'Graph build    ',
        'dijkstra':       'Dijkstra       ',
    }
    total = sum(timings.values())
    print("\n[BI-15 phase timings]  (avg over {} iters)".format(n_iters))
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


def _dijkstra_arrow(arrow_table, person1Id, person2Id, timings):
    """在 Arrow Table 上构建 igraph 无向图，用 Dijkstra 求最短路径权重之和。

    使用 numpy 向量化操作替代 Python 循环，减少图构建开销：
      - to_numpy() 从 Arrow 列零拷贝获取数组
      - np.unique + np.searchsorted 完成 O(n log n) 节点映射
    各子阶段耗时写入 timings 累计器。
    """
    if arrow_table.num_rows == 0:
        return -1

    # --- 阶段 1: Arrow → numpy 零拷贝 ---
    _t = time.perf_counter()
    src_arr    = arrow_table.column('src').to_numpy()
    dst_arr    = arrow_table.column('dst').to_numpy()
    weight_arr = arrow_table.column('weight').to_numpy()
    timings['arrow_to_numpy'] += time.perf_counter() - _t

    # --- 阶段 2: 节点映射（np.unique + np.searchsorted）---
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

    # --- 阶段 3: igraph 图构建 ---
    _t = time.perf_counter()
    g = ig.Graph(n=len(all_nodes), directed=False)
    # 优化：直接传 numpy stack，igraph 可接受 (N,2) 数组
    g.add_edges(list(zip(src_idx.tolist(), dst_idx.tolist())))
    # edges_np = np.column_stack([src_idx, dst_idx])
    # g.add_edges(edges_np)
    g.es['weight'] = weight_arr.tolist()
    timings['graph_build'] += time.perf_counter() - _t

    # --- 阶段 4: Dijkstra（igraph C 实现）---
    _t = time.perf_counter()
    dist = g.distances(
        source=int(p1_idx),
        target=int(p2_idx),
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

    # 构造 SET variable 语句列表（不含尾部分号，供 Python API 逐条执行）
    set_stmts = []
    for k, v in query_parameters.items():
        param_name, param_type = k.split(':')
        set_stmts.append(f"SET variable {param_name} = {format_value_duckdb(v, param_type)}")

    # 性能监控
    perf = subprocess.Popen(
        ["python3", "/work/machine_performance_indicators/monitor_system_perf.py", perf_file]
    )

    last_result = json.dumps([{"coalesce(min(w), -1)": -1}])
    con = _open_connection(set_stmts)
    timings = _make_timings()
    n_valid = 0

    # 从 perf_file 派生阶段计时 CSV 输出路径
    # perf_file 示例: system_performance/bi-15a/parameters-1.csv
    # 输出路径: output/phase_timings/bi-15a/parameters-1.csv
    perf_parts = Path(perf_file).parts
    phase_out_path = str(Path('output_orig', 'phase_timings', *perf_parts[1:]))

    start = time.time()

    try:
        for _ in range(10):
            # --- 阶段 0: DuckDB SQL 执行 ---
            _t = time.perf_counter()
            arrow_table = _extract_edges_arrow(con)
            timings['duckdb_sql'] += time.perf_counter() - _t

            if arrow_table.num_rows == 0:
                last_result = json.dumps([{"coalesce(min(w), -1)": -1}])
                continue

            result_val = _dijkstra_arrow(arrow_table, person1Id, person2Id, timings)  # igraph：最短路径
            last_result = json.dumps([{"coalesce(min(w), -1)": result_val}])
            n_valid += 1
    finally:
        con.close()

    duration = (time.time() - start) / 10
    perf.kill()

    _print_phase_timings(timings, max(n_valid, 1), out_csv_path=phase_out_path)

    return last_result, duration
