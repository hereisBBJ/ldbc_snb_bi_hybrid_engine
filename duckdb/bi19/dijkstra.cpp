/*
 * bi19/dijkstra.cpp
 *
 * pybind11 + igraph C API 实现的 Q19 最短路径核心：
 *   - 输入边表 (src, dst, weight)
 *   - 输入源点城市人员列表、终点城市人员列表
 *   - C++ 内部调用 igraph 构图并计算多源多目标最短路径
 *   - 返回与 Python 版一致的结果行：[{"f": ..., "t": ..., "w": ...}, ...]
 */

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <igraph/igraph.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <unordered_map>
#include <vector>

namespace py = pybind11;

py::list solve_bi19(
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> edge_srcs,
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> edge_dsts,
    py::array_t<double, py::array::c_style | py::array::forcecast> edge_weights,
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> src_nodes,
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> dst_nodes) {
    auto edge_src_buf = edge_srcs.unchecked<1>();
    auto edge_dst_buf = edge_dsts.unchecked<1>();
    auto edge_w_buf = edge_weights.unchecked<1>();
    auto src_node_buf = src_nodes.unchecked<1>();
    auto dst_node_buf = dst_nodes.unchecked<1>();

    const py::ssize_t n_edges = edge_src_buf.shape(0);
    const py::ssize_t n_src = src_node_buf.shape(0);
    const py::ssize_t n_dst = dst_node_buf.shape(0);

    py::list empty;
    if (n_edges == 0 || n_src == 0 || n_dst == 0) {
        return empty;
    }

    std::unordered_map<int64_t, int32_t> id2idx;
    id2idx.reserve(static_cast<size_t>(n_edges + n_src + n_dst) * 2 + 8);

    int32_t n_vertices = 0;
    auto intern = [&](int64_t id) -> int32_t {
        auto [it, inserted] = id2idx.emplace(id, n_vertices);
        if (inserted) {
            ++n_vertices;
        }
        return it->second;
    };

    for (py::ssize_t i = 0; i < n_edges; ++i) {
        intern(edge_src_buf(i));
        intern(edge_dst_buf(i));
    }
    for (py::ssize_t i = 0; i < n_src; ++i) {
        intern(src_node_buf(i));
    }
    for (py::ssize_t i = 0; i < n_dst; ++i) {
        intern(dst_node_buf(i));
    }

    std::vector<int64_t> src_ids(static_cast<size_t>(n_src));
    std::vector<int64_t> dst_ids(static_cast<size_t>(n_dst));
    std::vector<int32_t> src_idx(static_cast<size_t>(n_src));
    std::vector<int32_t> dst_idx(static_cast<size_t>(n_dst));

    for (py::ssize_t i = 0; i < n_src; ++i) {
        const int64_t id = src_node_buf(i);
        src_ids[static_cast<size_t>(i)] = id;
        auto it = id2idx.find(id);
        if (it == id2idx.end()) {
            return empty;
        }
        src_idx[static_cast<size_t>(i)] = it->second;
    }
    for (py::ssize_t i = 0; i < n_dst; ++i) {
        const int64_t id = dst_node_buf(i);
        dst_ids[static_cast<size_t>(i)] = id;
        auto it = id2idx.find(id);
        if (it == id2idx.end()) {
            return empty;
        }
        dst_idx[static_cast<size_t>(i)] = it->second;
    }

    struct ResultRow {
        int64_t f;
        int64_t t;
        double w;
    };

    std::vector<ResultRow> rows;
    rows.reserve(static_cast<size_t>(n_src) * static_cast<size_t>(n_dst));

    igraph_vector_t edges;
    igraph_vector_t weights;
    igraph_t graph;
    igraph_matrix_t distances;
    igraph_vs_t from_vs;
    igraph_vs_t to_vs;
    igraph_vector_t src_query;
    igraph_vector_t dst_query;

    if (igraph_vector_init(&edges, static_cast<igraph_integer_t>(n_edges * 2)) != IGRAPH_SUCCESS) {
        return empty;
    }
    if (igraph_vector_init(&weights, static_cast<igraph_integer_t>(n_edges)) != IGRAPH_SUCCESS) {
        igraph_vector_destroy(&edges);
        return empty;
    }

    for (py::ssize_t i = 0; i < n_edges; ++i) {
        const int32_t u = id2idx[edge_src_buf(i)];
        const int32_t v = id2idx[edge_dst_buf(i)];
        VECTOR(edges)[2 * i] = static_cast<igraph_real_t>(u);
        VECTOR(edges)[2 * i + 1] = static_cast<igraph_real_t>(v);
        VECTOR(weights)[i] = edge_w_buf(i);
    }

    if (igraph_create(&graph, &edges, n_vertices, IGRAPH_UNDIRECTED) != IGRAPH_SUCCESS) {
        igraph_vector_destroy(&weights);
        igraph_vector_destroy(&edges);
        return empty;
    }

    if (igraph_matrix_init(&distances, 0, 0) != IGRAPH_SUCCESS) {
        igraph_destroy(&graph);
        igraph_vector_destroy(&weights);
        igraph_vector_destroy(&edges);
        return empty;
    }

    if (igraph_vector_init(&src_query, static_cast<igraph_integer_t>(n_src)) != IGRAPH_SUCCESS) {
        igraph_matrix_destroy(&distances);
        igraph_destroy(&graph);
        igraph_vector_destroy(&weights);
        igraph_vector_destroy(&edges);
        return empty;
    }
    if (igraph_vector_init(&dst_query, static_cast<igraph_integer_t>(n_dst)) != IGRAPH_SUCCESS) {
        igraph_vector_destroy(&src_query);
        igraph_matrix_destroy(&distances);
        igraph_destroy(&graph);
        igraph_vector_destroy(&weights);
        igraph_vector_destroy(&edges);
        return empty;
    }

    for (py::ssize_t i = 0; i < n_src; ++i) {
        VECTOR(src_query)[i] = static_cast<igraph_real_t>(src_idx[static_cast<size_t>(i)]);
    }
    for (py::ssize_t i = 0; i < n_dst; ++i) {
        VECTOR(dst_query)[i] = static_cast<igraph_real_t>(dst_idx[static_cast<size_t>(i)]);
    }

    if (igraph_vs_vector(&from_vs, &src_query) != IGRAPH_SUCCESS) {
        igraph_vector_destroy(&src_query);
        igraph_vector_destroy(&dst_query);
        igraph_matrix_destroy(&distances);
        igraph_destroy(&graph);
        igraph_vector_destroy(&weights);
        igraph_vector_destroy(&edges);
        return empty;
    }
    if (igraph_vs_vector(&to_vs, &dst_query) != IGRAPH_SUCCESS) {
        igraph_vs_destroy(&from_vs);
        igraph_vector_destroy(&src_query);
        igraph_vector_destroy(&dst_query);
        igraph_matrix_destroy(&distances);
        igraph_destroy(&graph);
        igraph_vector_destroy(&weights);
        igraph_vector_destroy(&edges);
        return empty;
    }

    const int rc = igraph_shortest_paths_dijkstra(
        &graph,
        &distances,
        from_vs,
        to_vs,
        &weights,
        IGRAPH_OUT
    );

    if (rc != IGRAPH_SUCCESS) {
        igraph_vs_destroy(&from_vs);
        igraph_vs_destroy(&to_vs);
        igraph_vector_destroy(&src_query);
        igraph_vector_destroy(&dst_query);
        igraph_matrix_destroy(&distances);
        igraph_destroy(&graph);
        igraph_vector_destroy(&weights);
        igraph_vector_destroy(&edges);
        return empty;
    }

    const double inf = std::numeric_limits<double>::infinity();
    for (py::ssize_t i = 0; i < n_src; ++i) {
        for (py::ssize_t j = 0; j < n_dst; ++j) {
            const double d = MATRIX(distances, i, j);
            if (d != inf) {
                rows.push_back({src_ids[static_cast<size_t>(i)], dst_ids[static_cast<size_t>(j)], d});
            }
        }
    }

    if (rows.empty()) {
        return empty;
    }

    double best_w = rows.front().w;
    for (const auto& row : rows) {
        if (row.w < best_w) {
            best_w = row.w;
        }
    }

    std::vector<ResultRow> best_rows;
    for (const auto& row : rows) {
        if (row.w == best_w) {
            best_rows.push_back(row);
        }
    }

    std::sort(best_rows.begin(), best_rows.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.f != b.f) {
            return a.f < b.f;
        }
        return a.t < b.t;
    });

    py::list result;
    for (const auto& row : best_rows) {
        py::dict item;
        item["f"] = row.f;
        item["t"] = row.t;
        item["w"] = row.w;
        result.append(item);
    }

    igraph_vs_destroy(&from_vs);
    igraph_vs_destroy(&to_vs);
    igraph_vector_destroy(&src_query);
    igraph_vector_destroy(&dst_query);
    igraph_matrix_destroy(&distances);
    igraph_destroy(&graph);
    igraph_vector_destroy(&weights);
    igraph_vector_destroy(&edges);

    return result;
}

PYBIND11_MODULE(bi19_dijkstra_cpp, m) {
    m.doc() = "BI-19 shortest paths C++ backend via pybind11";
    m.def("solve_bi19", &solve_bi19, py::arg("edge_srcs"), py::arg("edge_dsts"), py::arg("edge_weights"), py::arg("src_nodes"), py::arg("dst_nodes"));
}
