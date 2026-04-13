/*
 * bi19/dijkstra_cached_igraph.cpp
 *
 * pybind11 + igraph C API, with in-process graph cache:
 *   - build_graph(...) builds/refreshes graph + weights once per cache key
 *   - solve_cached(...) reuses cached graph for repeated source/target queries
 */

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>

#include <igraph/igraph.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace py = pybind11;

namespace {

struct CachedGraph {
    igraph_t graph;
    igraph_vector_t weights;
    bool has_graph = false;
    bool has_weights = false;
    std::unordered_map<int64_t, igraph_integer_t> node_to_idx;
    std::string cache_key;

    void clear() {
        if (has_graph) {
            igraph_destroy(&graph);
            has_graph = false;
        }
        if (has_weights) {
            igraph_vector_destroy(&weights);
            has_weights = false;
        }
        node_to_idx.clear();
        cache_key.clear();
    }

    ~CachedGraph() {
        clear();
    }
};

CachedGraph CACHE;

struct DedupedSelector {
    std::vector<int64_t> unique_ids;
    std::vector<igraph_integer_t> original_to_unique;
};

DedupedSelector dedupe_ids(const int64_t* ids, py::ssize_t n) {
    DedupedSelector out;
    out.unique_ids.reserve(static_cast<size_t>(n));
    out.original_to_unique.resize(static_cast<size_t>(n));

    std::unordered_map<int64_t, igraph_integer_t> first_pos;
    first_pos.reserve(static_cast<size_t>(n));

    for (py::ssize_t i = 0; i < n; ++i) {
        const int64_t id = ids[i];
        auto it = first_pos.find(id);
        if (it == first_pos.end()) {
            const igraph_integer_t pos = static_cast<igraph_integer_t>(out.unique_ids.size());
            first_pos.emplace(id, pos);
            out.unique_ids.push_back(id);
            out.original_to_unique[static_cast<size_t>(i)] = pos;
        } else {
            out.original_to_unique[static_cast<size_t>(i)] = it->second;
        }
    }

    return out;
}

}  // namespace

py::bool_ build_graph(
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> edge_srcs,
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> edge_dsts,
    py::array_t<double, py::array::c_style | py::array::forcecast> edge_weights,
    const std::string& cache_key) {
    if (CACHE.has_graph && CACHE.cache_key == cache_key) {
        return py::bool_(false);
    }

    auto src_buf = edge_srcs.unchecked<1>();
    auto dst_buf = edge_dsts.unchecked<1>();
    auto w_buf = edge_weights.unchecked<1>();
    const py::ssize_t n_edges = src_buf.shape(0);

    CACHE.clear();

    if (n_edges == 0) {
        CACHE.cache_key = cache_key;
        return py::bool_(true);
    }

    CACHE.node_to_idx.reserve(static_cast<size_t>(n_edges * 2 + 8));
    igraph_integer_t n_vertices = 0;

    auto intern = [&](int64_t id) -> igraph_integer_t {
        auto [it, inserted] = CACHE.node_to_idx.emplace(id, n_vertices);
        if (inserted) {
            ++n_vertices;
        }
        return it->second;
    };

    for (py::ssize_t i = 0; i < n_edges; ++i) {
        intern(src_buf(i));
        intern(dst_buf(i));
    }

    igraph_vector_t edges;
    if (igraph_vector_init(&edges, static_cast<igraph_integer_t>(n_edges * 2)) != IGRAPH_SUCCESS) {
        throw std::runtime_error("igraph_vector_init(edges) failed");
    }

    if (igraph_vector_init(&CACHE.weights, static_cast<igraph_integer_t>(n_edges)) != IGRAPH_SUCCESS) {
        igraph_vector_destroy(&edges);
        throw std::runtime_error("igraph_vector_init(weights) failed");
    }
    CACHE.has_weights = true;

    for (py::ssize_t i = 0; i < n_edges; ++i) {
        const igraph_integer_t u = CACHE.node_to_idx[src_buf(i)];
        const igraph_integer_t v = CACHE.node_to_idx[dst_buf(i)];
        VECTOR(edges)[2 * i] = static_cast<igraph_real_t>(u);
        VECTOR(edges)[2 * i + 1] = static_cast<igraph_real_t>(v);
        VECTOR(CACHE.weights)[i] = w_buf(i);
    }

    if (igraph_create(&CACHE.graph, &edges, n_vertices, IGRAPH_UNDIRECTED) != IGRAPH_SUCCESS) {
        igraph_vector_destroy(&edges);
        CACHE.clear();
        throw std::runtime_error("igraph_create failed");
    }
    CACHE.has_graph = true;
    CACHE.cache_key = cache_key;

    igraph_vector_destroy(&edges);
    return py::bool_(true);
}

py::list solve_cached(
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> src_nodes,
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> dst_nodes) {
    py::list empty;
    if (!CACHE.has_graph) {
        return empty;
    }

    auto src_buf = src_nodes.unchecked<1>();
    auto dst_buf = dst_nodes.unchecked<1>();
    const py::ssize_t n_src = src_buf.shape(0);
    const py::ssize_t n_dst = dst_buf.shape(0);

    if (n_src == 0 || n_dst == 0) {
        return empty;
    }

    std::vector<int64_t> src_ids(static_cast<size_t>(n_src));
    std::vector<int64_t> dst_ids(static_cast<size_t>(n_dst));
    for (py::ssize_t i = 0; i < n_src; ++i) {
        src_ids[static_cast<size_t>(i)] = src_buf(i);
    }
    for (py::ssize_t i = 0; i < n_dst; ++i) {
        dst_ids[static_cast<size_t>(i)] = dst_buf(i);
    }

    const DedupedSelector src_sel = dedupe_ids(src_ids.data(), n_src);
    const DedupedSelector dst_sel = dedupe_ids(dst_ids.data(), n_dst);

    std::vector<int64_t> missing;
    std::unordered_set<int64_t> missing_seen;
    missing.reserve(static_cast<size_t>(src_sel.unique_ids.size() + dst_sel.unique_ids.size()));
    for (const int64_t id : src_sel.unique_ids) {
        if (CACHE.node_to_idx.find(id) == CACHE.node_to_idx.end() && missing_seen.insert(id).second) {
            missing.push_back(id);
        }
    }
    for (const int64_t id : dst_sel.unique_ids) {
        if (CACHE.node_to_idx.find(id) == CACHE.node_to_idx.end() && missing_seen.insert(id).second) {
            missing.push_back(id);
        }
    }

    if (!missing.empty()) {
        const igraph_integer_t start = igraph_vcount(&CACHE.graph);
        if (igraph_add_vertices(&CACHE.graph, static_cast<igraph_integer_t>(missing.size()), nullptr) != IGRAPH_SUCCESS) {
            throw std::runtime_error("igraph_add_vertices failed");
        }
        for (igraph_integer_t i = 0; i < static_cast<igraph_integer_t>(missing.size()); ++i) {
            CACHE.node_to_idx[missing[static_cast<size_t>(i)]] = start + i;
        }
    }

    igraph_vector_t src_query;
    igraph_vector_t dst_query;
    if (igraph_vector_init(&src_query, static_cast<igraph_integer_t>(src_sel.unique_ids.size())) != IGRAPH_SUCCESS) {
        throw std::runtime_error("igraph_vector_init(src_query) failed");
    }
    if (igraph_vector_init(&dst_query, static_cast<igraph_integer_t>(dst_sel.unique_ids.size())) != IGRAPH_SUCCESS) {
        igraph_vector_destroy(&src_query);
        throw std::runtime_error("igraph_vector_init(dst_query) failed");
    }

    for (size_t i = 0; i < src_sel.unique_ids.size(); ++i) {
        VECTOR(src_query)[static_cast<igraph_integer_t>(i)] =
            static_cast<igraph_real_t>(CACHE.node_to_idx[src_sel.unique_ids[i]]);
    }
    for (size_t i = 0; i < dst_sel.unique_ids.size(); ++i) {
        VECTOR(dst_query)[static_cast<igraph_integer_t>(i)] =
            static_cast<igraph_real_t>(CACHE.node_to_idx[dst_sel.unique_ids[i]]);
    }

    igraph_vs_t from_vs;
    igraph_vs_t to_vs;
    if (igraph_vs_vector(&from_vs, &src_query) != IGRAPH_SUCCESS) {
        igraph_vector_destroy(&src_query);
        igraph_vector_destroy(&dst_query);
        throw std::runtime_error("igraph_vs_vector(from) failed");
    }
    if (igraph_vs_vector(&to_vs, &dst_query) != IGRAPH_SUCCESS) {
        igraph_vs_destroy(&from_vs);
        igraph_vector_destroy(&src_query);
        igraph_vector_destroy(&dst_query);
        throw std::runtime_error("igraph_vs_vector(to) failed");
    }

    igraph_matrix_t distances;
    if (igraph_matrix_init(&distances, 0, 0) != IGRAPH_SUCCESS) {
        igraph_vs_destroy(&from_vs);
        igraph_vs_destroy(&to_vs);
        igraph_vector_destroy(&src_query);
        igraph_vector_destroy(&dst_query);
        throw std::runtime_error("igraph_matrix_init(distances) failed");
    }

    const int rc = igraph_shortest_paths_dijkstra(
        &CACHE.graph,
        &distances,
        from_vs,
        to_vs,
        &CACHE.weights,
        IGRAPH_OUT
    );

    igraph_vs_destroy(&from_vs);
    igraph_vs_destroy(&to_vs);
    igraph_vector_destroy(&src_query);
    igraph_vector_destroy(&dst_query);

    if (rc != IGRAPH_SUCCESS) {
        igraph_matrix_destroy(&distances);
        throw std::runtime_error("igraph_shortest_paths_dijkstra failed");
    }

    struct ResultRow {
        int64_t f;
        int64_t t;
        double w;
    };

    std::vector<ResultRow> finite;
    finite.reserve(static_cast<size_t>(n_src * n_dst));
    const double inf = std::numeric_limits<double>::infinity();
    for (py::ssize_t i = 0; i < n_src; ++i) {
        const igraph_integer_t src_pos = src_sel.original_to_unique[static_cast<size_t>(i)];
        for (py::ssize_t j = 0; j < n_dst; ++j) {
            const igraph_integer_t dst_pos = dst_sel.original_to_unique[static_cast<size_t>(j)];
            const double d = MATRIX(distances, src_pos, dst_pos);
            if (d != inf) {
                finite.push_back({
                    src_ids[static_cast<size_t>(i)],
                    dst_ids[static_cast<size_t>(j)],
                    d
                });
            }
        }
    }

    igraph_matrix_destroy(&distances);

    if (finite.empty()) {
        return empty;
    }

    double best_w = finite.front().w;
    for (const auto& row : finite) {
        if (row.w < best_w) {
            best_w = row.w;
        }
    }

    std::vector<ResultRow> best_rows;
    best_rows.reserve(finite.size());
    for (const auto& row : finite) {
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

    return result;
}

void clear_cache() {
    CACHE.clear();
}

PYBIND11_MODULE(bi19_igraph_cached_cpp, m) {
    m.doc() = "BI-19 cached igraph backend via pybind11";
    m.def("build_graph", &build_graph, py::arg("edge_srcs"), py::arg("edge_dsts"), py::arg("edge_weights"), py::arg("cache_key"));
    m.def("solve_cached", &solve_cached, py::arg("src_nodes"), py::arg("dst_nodes"));
    m.def("clear_cache", &clear_cache);
}
