"""
bi15/setup.py —— 编译 BI-15 pybind11 扩展

用法（在 bi15/ 目录下执行）：
    python3 setup.py build_ext --inplace

编译产物：
    bi15_dijkstra_cpp*.so
    bi15_dijkstra_igraph_cpp*.so
（均在 bi15/ 目录内）
由 bi15/__init__.py 将 bi15/ 加入 sys.path 后可直接 import。
"""

from setuptools import Extension, setup
import pybind11

ext_dijkstra = Extension(
    name="bi15_dijkstra_cpp",
    sources=["dijkstra.cpp"],
    include_dirs=[pybind11.get_include()],
    language="c++",
    extra_compile_args=[
        "-O3",
        "-std=c++17",
        "-fvisibility=hidden",
        "-ffast-math",
        "-march=native",
    ],
)

ext_igraph = Extension(
    name="bi15_dijkstra_igraph_cpp",
    sources=["dijkstra_igraph.cpp"],
    include_dirs=[pybind11.get_include()],
    libraries=["igraph"],
    language="c++",
    extra_compile_args=[
        "-O3",
        "-std=c++17",
        "-fvisibility=hidden",
        "-ffast-math",
        "-march=native",
    ],
)

setup(
    name="bi15_dijkstra_extensions",
    ext_modules=[ext_dijkstra, ext_igraph],
)
