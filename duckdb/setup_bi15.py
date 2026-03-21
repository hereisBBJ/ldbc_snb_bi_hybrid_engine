"""
setup_bi15.py —— 编译 bi15_dijkstra_cpp pybind11 扩展

用法（在 ldbc_snb_bi-main/duckdb/ 目录下执行）：
    python3 setup_bi15.py build_ext --inplace

编译产物：bi15_dijkstra_cpp*.so（同目录），可直接被 bi15_pybind.py import。
"""

from setuptools import Extension, setup
import pybind11

ext = Extension(
    name="bi15_dijkstra_cpp",
    sources=["bi15_dijkstra.cpp"],
    include_dirs=[pybind11.get_include()],
    language="c++",
    extra_compile_args=[
        "-O3",
        "-std=c++17",
        "-fvisibility=hidden",   # 减小符号表，与 pybind11 最佳实践一致
        "-ffast-math",           # 浮点运算加速（Dijkstra 中 double 比较）
        "-march=native",         # 针对宿主 CPU 优化
    ],
)

setup(
    name="bi15_dijkstra_cpp",
    ext_modules=[ext],
)
