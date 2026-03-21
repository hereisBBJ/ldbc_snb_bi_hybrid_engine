"""
bi15 package —— BI-15 查询加速模块
=====================================
目录结构：
  bi15/
    __init__.py            本文件：包入口，sys.path 修正 + 导出 run_query_15
    dijkstra.cpp           C++17 Dijkstra + pybind11 绑定源码
    setup.py               编译脚本（build_ext --inplace）
    pybind_backend.py      主入口：pybind11 C++ Dijkstra，回退 igraph
    igraph_backend.py      Arrow + igraph Dijkstra（备用）
    igraph_json_backend.py DuckDB CLI + JSON + igraph（备用）
    bi-15-test.sql         测试 SQL
    bi-15-test-explain     测试 EXPLAIN 输出
    bi-15-test-explain.json测试 EXPLAIN JSON

编译 C++ 扩展（在 duckdb/ 目录下执行）：
    python3 -m pip install pybind11
    cd bi15 && python3 setup.py build_ext --inplace
"""

import sys
import os

# 将 bi15/ 目录加入 sys.path，确保编译产物 bi15_dijkstra_cpp*.so 可被 import
_here = os.path.dirname(os.path.abspath(__file__))
if _here not in sys.path:
    sys.path.insert(0, _here)

# 公开接口：只暴露 run_query_15
from .igraph_backend import run_query_15  # noqa: E402

__all__ = ["run_query_15"]
