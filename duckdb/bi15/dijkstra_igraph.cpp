/*
 * bi15/dijkstra_igraph.cpp
 *
 * pybind11 + igraph C API 实现的最短路径模块。
 *
 * 接口：
 *   dijkstra_shortest_path_igraph(srcs, dsts, weights, p1, p2) -> float
 *
 * 说明：
 *   - 输入为无向图边列表（每条边一行）
 *   - 内部将 PersonId 压缩为连续索引后，使用 igraph_distances_dijkstra
 *   - 不可达或输入不合法时返回 -1.0
 */

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>

#include <igraph/igraph.h>

#include <cstdint>
#include <limits>
#include <unordered_map>
#include <vector>

namespace py = pybind11;

double dijkstra_shortest_path_igraph(
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> srcs,
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> dsts,
    py::array_t<double, py::array::c_style | py::array::forcecast> weights,
    int64_t p1,
    int64_t p2) {
    auto src_buf = srcs.unchecked<1>();
    auto dst_buf = dsts.unchecked<1>();
    auto wgt_buf = weights.unchecked<1>();
    const py::ssize_t n_edges = src_buf.shape(0);

    if (n_edges == 0) {
        return -1.0;
    }
    if (p1 == p2) {
        return 0.0;
    }

    // 1) PersonId -> 连续索引
    std::unordered_map<int64_t, igraph_integer_t> id2idx;
    id2idx.reserve(static_cast<size_t>(n_edges) * 2 + 4);

    igraph_integer_t n_vertices = 0;
    auto intern = [&](int64_t id) -> igraph_integer_t {
        auto res = id2idx.emplace(id, n_vertices);
        if (res.second) {
            ++n_vertices;
        }
        return res.first->second;
    };

    for (py::ssize_t i = 0; i < n_edges; ++i) {
        intern(src_buf(i));
        intern(dst_buf(i));
    }

    auto it1 = id2idx.find(p1);
    auto it2 = id2idx.find(p2);
    if (it1 == id2idx.end() || it2 == id2idx.end()) {
        return -1.0;
    }
    const igraph_integer_t s = it1->second;
    const igraph_integer_t t = it2->second;

    // 2) 构造 igraph 图
    igraph_vector_t edges;
    igraph_vector_t w;
    igraph_t g;
    igraph_matrix_t dist_mat;
    igraph_vs_t from_vs;
    igraph_vs_t to_vs;

    const igraph_integer_t edge_vec_len = static_cast<igraph_integer_t>(n_edges * 2);
    if (igraph_vector_init(&edges, edge_vec_len) != IGRAPH_SUCCESS) {
        return -1.0;
    }
    if (igraph_vector_init(&w, static_cast<igraph_integer_t>(n_edges)) != IGRAPH_SUCCESS) {
        igraph_vector_destroy(&edges);
        return -1.0;
    }

    for (py::ssize_t i = 0; i < n_edges; ++i) {
        const igraph_integer_t u = id2idx[src_buf(i)];
        const igraph_integer_t v = id2idx[dst_buf(i)];
        VECTOR(edges)[2 * i] = static_cast<igraph_real_t>(u);
        VECTOR(edges)[2 * i + 1] = static_cast<igraph_real_t>(v);
        VECTOR(w)[i] = wgt_buf(i);
    }

    if (igraph_create(&g, &edges, n_vertices, IGRAPH_UNDIRECTED) != IGRAPH_SUCCESS) {
        igraph_vector_destroy(&w);
        igraph_vector_destroy(&edges);
        return -1.0;
    }
    if (igraph_matrix_init(&dist_mat, 0, 0) != IGRAPH_SUCCESS) {
        igraph_destroy(&g);
        igraph_vector_destroy(&w);
        igraph_vector_destroy(&edges);
        return -1.0;
    }

    igraph_vs_1(&from_vs, s);
    igraph_vs_1(&to_vs, t);

    const int rc = igraph_shortest_paths_dijkstra(
        &g,
        &dist_mat,
        from_vs,
        to_vs,
        &w,
        IGRAPH_OUT
    );

    double ans = -1.0;
    if (rc == IGRAPH_SUCCESS && igraph_matrix_nrow(&dist_mat) > 0 && igraph_matrix_ncol(&dist_mat) > 0) {
        const double d = MATRIX(dist_mat, 0, 0);
        if (d != IGRAPH_INFINITY && d < std::numeric_limits<double>::infinity()) {
            ans = d;
        }
    }

    igraph_vs_destroy(&from_vs);
    igraph_vs_destroy(&to_vs);
    igraph_matrix_destroy(&dist_mat);
    igraph_destroy(&g);
    igraph_vector_destroy(&w);
    igraph_vector_destroy(&edges);

    return ans;
}

PYBIND11_MODULE(bi15_dijkstra_igraph_cpp, m) {
    m.doc() = "BI-15 Dijkstra via igraph C API";
    m.def(
        "dijkstra_shortest_path_igraph",
        &dijkstra_shortest_path_igraph,
        py::arg("srcs"),
        py::arg("dsts"),
        py::arg("weights"),
        py::arg("p1"),
        py::arg("p2")
    );
}
