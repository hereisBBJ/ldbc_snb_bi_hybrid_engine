/*
 * bi15_dijkstra.cpp
 *
 * pybind11 封装的 C++ Dijkstra 模块，用于 BI-15 查询加速。
 *
 * 接口：
 *   dijkstra_shortest_path(srcs, dsts, weights, p1, p2) -> float
 *
 *   srcs    : np.ndarray[int64]  —— 边起点 PersonId 数组
 *   dsts    : np.ndarray[int64]  —— 边终点 PersonId 数组（每条边只需一行，无向图）
 *   weights : np.ndarray[float64]—— 对应边权（10 / (w_mm + 10)）
 *   p1, p2  : int64              —— 起点 / 终点 PersonId
 *
 *   返回最短路径权重之和；若不可达则返回 -1.0。
 *
 * 编译：
 *   python3 setup_bi15.py build_ext --inplace
 */

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <cstdint>
#include <limits>
#include <queue>
#include <unordered_map>
#include <vector>

namespace py = pybind11;

// ---------------------------------------------------------------------------
// dijkstra_shortest_path
// ---------------------------------------------------------------------------
double dijkstra_shortest_path(
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> srcs,
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> dsts,
    py::array_t<double,  py::array::c_style | py::array::forcecast> weights,
    int64_t p1,
    int64_t p2)
{
    auto src_buf = srcs.unchecked<1>();
    auto dst_buf = dsts.unchecked<1>();
    auto wgt_buf = weights.unchecked<1>();
    const py::ssize_t n_edges = src_buf.shape(0);

    if (n_edges == 0) return -1.0;
    if (p1 == p2)     return 0.0;

    // -----------------------------------------------------------------------
    // 1. 压缩节点 ID → 连续整数索引
    // -----------------------------------------------------------------------
    std::unordered_map<int64_t, int32_t> id2idx;
    id2idx.reserve(static_cast<size_t>(n_edges) * 2 + 4);

    int32_t cnt = 0;
    auto intern = [&](int64_t id) -> int32_t {
        auto res = id2idx.emplace(id, cnt);
        if (res.second) ++cnt;
        return res.first->second;
    };

    for (py::ssize_t i = 0; i < n_edges; ++i) {
        intern(src_buf(i));
        intern(dst_buf(i));
    }

    // -----------------------------------------------------------------------
    // 2. 检查起止点是否存在于图中
    // -----------------------------------------------------------------------
    auto it1 = id2idx.find(p1);
    auto it2 = id2idx.find(p2);
    if (it1 == id2idx.end() || it2 == id2idx.end()) return -1.0;

    const int32_t s = it1->second;
    const int32_t t = it2->second;

    // -----------------------------------------------------------------------
    // 3. 建邻接表（无向图：双向存边）
    // -----------------------------------------------------------------------
    std::vector<std::vector<std::pair<int32_t, double>>> adj(
        static_cast<size_t>(cnt));

    for (py::ssize_t i = 0; i < n_edges; ++i) {
        int32_t u = id2idx[src_buf(i)];
        int32_t v = id2idx[dst_buf(i)];
        double  w = wgt_buf(i);
        adj[u].emplace_back(v, w);
        adj[v].emplace_back(u, w);
    }

    // -----------------------------------------------------------------------
    // 4. Dijkstra（小根堆）
    // -----------------------------------------------------------------------
    const double INF = std::numeric_limits<double>::infinity();
    std::vector<double> dist(static_cast<size_t>(cnt), INF);
    dist[s] = 0.0;

    // pair: (distance, node_index)
    using PD = std::pair<double, int32_t>;
    std::priority_queue<PD, std::vector<PD>, std::greater<PD>> pq;
    pq.emplace(0.0, s);

    while (!pq.empty()) {
        auto [d, u] = pq.top();
        pq.pop();
        if (d > dist[u]) continue;   // 过期条目
        if (u == t)      break;      // 已到达终点
        for (auto [v, w] : adj[u]) {
            double nd = d + w;
            if (nd < dist[v]) {
                dist[v] = nd;
                pq.emplace(nd, v);
            }
        }
    }

    return dist[t] < INF ? dist[t] : -1.0;
}

// ---------------------------------------------------------------------------
// pybind11 模块注册
// ---------------------------------------------------------------------------
PYBIND11_MODULE(bi15_dijkstra_cpp, m) {
    m.doc() = "BI-15 Dijkstra C++ extension via pybind11";
    m.def(
        "dijkstra_shortest_path",
        &dijkstra_shortest_path,
        py::arg("srcs"),
        py::arg("dsts"),
        py::arg("weights"),
        py::arg("p1"),
        py::arg("p2"),
        R"doc(
        在边列表上运行 Dijkstra 最短路径算法。

        参数
        ----
        srcs, dsts : np.ndarray[int64]   边端点（无向图，每对节点一行）
        weights    : np.ndarray[float64] 对应边权
        p1, p2     : int                 起点 / 终点 PersonId

        返回
        ----
        float  最短路径权重之和；不可达时返回 -1.0
        )doc"
    );
}
