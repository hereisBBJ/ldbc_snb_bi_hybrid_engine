"""
bi15_pybind.py
BI-15 加速实现（pybind11/C++ 版本）：
  - DuckDB Python API 负责非迭代部分（myForums 过滤 → mm 交互聚合 → path 边权计算），
    通过 Arrow 零拷贝导出边表
  - bi15_dijkstra_cpp（pybind11 C++ 扩展）负责 Dijkstra 最短路径
    * 纯 C++ std::priority_queue，无 igraph 依赖
    * numpy 数组直接传指针，无额外拷贝
  - 若 bi15_dijkstra_cpp 未编译，自动回退到 igraph 实现

编译扩展：
    cd ldbc_snb_bi-main/duckdb
    python3 setup_bi15.py build_ext --inplace
"""

import subprocess
import json
import time
from pathlib import Path

import duckdb
import numpy as np

# ---------------------------------------------------------------------------
# 优先使用 pybind11 C++ 扩展；若尚未编译则回退 igraph
# ---------------------------------------------------------------------------
try:
    import bi15_dijkstra_cpp
    _BACKEND = "pybind11"
except ImportError:
    import igraph as ig
    _BACKEND = "igraph"

from db_config import db_file, format_value_duckdb

# ---------------------------------------------------------------------------
# 非迭代 SQL：子图过滤 + 匹配 + 边权计算，输出 (src, dst, weight)
# （与 bi15_igraph.py 完全相同，只保留一份）
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
where pp.person1id < pp.person2id
"""


def _open_connection(set_stmts):
    """打开只读 DuckDB 连接，执行 SET variable 语句列表。"""
    con = duckdb.connect(db_file, read_only=True)
    con.execute("SET GLOBAL TimeZone = 'Etc/UTC'")
    for stmt in set_stmts:
        con.execute(stmt)
    return con


def _make_timings():
    return {
        'duckdb_sql':     0.0,   # DuckDB 执行 SQL + Arrow Table
        'arrow_to_numpy': 0.0,   # Arrow 列 → numpy（零拷贝 .to_numpy()）
        'dijkstra_cpp':   0.0,   # C++ Dijkstra（含节点映射 + 堆）
    }


def _print_phase_timings(timings, n_iters, backend, out_csv_path=None):
    labels = {
        'duckdb_sql':     'DuckDB SQL     ',
        'arrow_to_numpy': 'Arrow→numpy    ',
        'dijkstra_cpp':   f'Dijkstra({backend:<7})',
    }
    total = sum(timings.values())
    print(f"\n[BI-15 phase timings / backend={backend}]  (avg over {n_iters} iters)")
    print(f"  {'Phase':<24} {'avg(ms)':>10} {'total(ms)':>11} {'ratio':>8}")
    print("  " + "-" * 56)
    for key, label in labels.items():
        t = timings[key]
        avg_ms  = t / n_iters * 1000
        tot_ms  = t * 1000
        ratio   = t / total * 100 if total > 0 else 0.0
        print(f"  {label:<24} {avg_ms:>10.3f} {tot_ms:>11.3f} {ratio:>7.1f}%")
    print("  " + "-" * 56)
    tot_avg_ms = total / n_iters * 1000
    print(f"  {'TOTAL':<24} {tot_avg_ms:>10.3f} {total*1000:>11.3f} {'100.0':>7}%")
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


# ---------------------------------------------------------------------------
# 核心：从 Arrow Table 运行最短路径（pybind11 C++ 分支）
# ---------------------------------------------------------------------------
def _dijkstra_cpp(arrow_table, person1Id, person2Id, timings):
    """Arrow Table → numpy → C++ Dijkstra，返回路径权重或 -1。"""
    if arrow_table.num_rows == 0:
        return -1

    # Arrow → numpy（零拷贝）
    _t = time.perf_counter()
    src_arr    = arrow_table.column('src').to_numpy()
    dst_arr    = arrow_table.column('dst').to_numpy()
    weight_arr = arrow_table.column('weight').to_numpy()
    timings['arrow_to_numpy'] += time.perf_counter() - _t

    # C++ Dijkstra
    _t = time.perf_counter()
    dist = bi15_dijkstra_cpp.dijkstra_shortest_path(
        src_arr, dst_arr, weight_arr,
        int(person1Id), int(person2Id)
    )
    timings['dijkstra_cpp'] += time.perf_counter() - _t

    return dist


# ---------------------------------------------------------------------------
# 回退：igraph 分支（bi15_dijkstra_cpp 编译失败时使用）
# ---------------------------------------------------------------------------
def _dijkstra_igraph(arrow_table, person1Id, person2Id, timings):
    """Arrow Table → numpy → igraph Dijkstra 回退路径。"""
    if arrow_table.num_rows == 0:
        return -1

    _t = time.perf_counter()
    src_arr    = arrow_table.column('src').to_numpy()
    dst_arr    = arrow_table.column('dst').to_numpy()
    weight_arr = arrow_table.column('weight').to_numpy()
    timings['arrow_to_numpy'] += time.perf_counter() - _t

    _t = time.perf_counter()
    all_nodes = np.unique(np.concatenate([src_arr, dst_arr]))
    p1_idx = int(np.searchsorted(all_nodes, person1Id))
    p2_idx = int(np.searchsorted(all_nodes, person2Id))
    if p1_idx >= len(all_nodes) or all_nodes[p1_idx] != person1Id:
        timings['dijkstra_cpp'] += time.perf_counter() - _t
        return -1
    if p2_idx >= len(all_nodes) or all_nodes[p2_idx] != person2Id:
        timings['dijkstra_cpp'] += time.perf_counter() - _t
        return -1
    src_idx = np.searchsorted(all_nodes, src_arr)
    dst_idx = np.searchsorted(all_nodes, dst_arr)

    g = ig.Graph(n=len(all_nodes), directed=False)
    g.add_edges(list(zip(src_idx.tolist(), dst_idx.tolist())))
    g.es['weight'] = weight_arr.tolist()
    dist = g.distances(source=p1_idx, target=p2_idx, weights='weight')[0][0]
    timings['dijkstra_cpp'] += time.perf_counter() - _t

    return dist if dist != float('inf') else -1


# ---------------------------------------------------------------------------
# 公共入口：run_query_15（与 bi15_igraph.py 接口完全一致）
# ---------------------------------------------------------------------------
def run_query_15(query_variant, query_parameters, perf_file):
    """
    BI-15 入口，返回 (result_json_str, duration_seconds)。
    执行 10 次取平均耗时，与其他查询保持一致。
    使用 pybind11 C++ Dijkstra 加速（如可用）。
    """
    param_dict = {k.split(':')[0]: v for k, v in query_parameters.items()}
    person1Id  = int(param_dict['person1Id'])
    person2Id  = int(param_dict['person2Id'])

    set_stmts = []
    for k, v in query_parameters.items():
        param_name, param_type = k.split(':')
        set_stmts.append(
            f"SET variable {param_name} = {format_value_duckdb(v, param_type)}"
        )

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
            # DuckDB SQL → Arrow Table
            _t = time.perf_counter()
            arrow_table = con.execute(EDGE_SELECT_SQL).fetch_arrow_table()
            timings['duckdb_sql'] += time.perf_counter() - _t

            if arrow_table.num_rows == 0:
                last_result = json.dumps([{"coalesce(min(w), -1)": -1}])
                continue

            # 根据后端选择 Dijkstra 实现
            if _BACKEND == "pybind11":
                dist = _dijkstra_cpp(arrow_table, person1Id, person2Id, timings)
            else:
                dist = _dijkstra_igraph(arrow_table, person1Id, person2Id, timings)

            last_result = json.dumps([{"coalesce(min(w), -1)": dist}])
            n_valid += 1
    finally:
        con.close()

    duration = (time.time() - start) / 10
    perf.kill()

    _print_phase_timings(timings, max(n_valid, 1), _BACKEND, out_csv_path=phase_out_path)
    return last_result, duration
