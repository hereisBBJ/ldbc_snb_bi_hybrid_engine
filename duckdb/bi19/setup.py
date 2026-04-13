"""
bi19/setup.py —— 编译 BI-19 的 pybind11 扩展

用法（在 bi19/ 目录下执行）：
    python3 setup.py build_ext --inplace
"""

from setuptools import Extension, setup
import pybind11

ext_dijkstra = Extension(
    name="bi19_dijkstra_cpp",
    sources=["dijkstra.cpp"],
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

ext_cached_igraph = Extension(
    name="bi19_igraph_cached_cpp",
    sources=["dijkstra_cached_igraph.cpp"],
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
    name="bi19_cpp_extensions",
    ext_modules=[ext_dijkstra, ext_cached_igraph],
)
