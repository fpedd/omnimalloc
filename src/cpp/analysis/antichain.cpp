//
// SPDX-License-Identifier: Apache-2.0
//

#include "antichain.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <numeric>
#include <span>
#include <stdexcept>
#include <vector>

#include "clock.hpp"
#include "common/parallel.hpp"
#include "linearize.hpp"

// Weighted Dilworth via min flow: the minimum s->t flow whose split nodes each
// carry their size equals the max-weight antichain, by LP duality. Costs two
// Dinic runs, saturating the demands then draining back to the minimum.

namespace omnimalloc {

namespace {

// Dinic max flow; edge 2e and 2e + 1 are mutual reverses.
class Dinic {
 public:
  explicit Dinic(size_t num_nodes)
      : adjacency_(num_nodes), levels_(num_nodes), cursors_(num_nodes) {}

  size_t add_edge(size_t tail, size_t head, int64_t capacity) {
    const size_t id = edges_.size();
    adjacency_[tail].push_back(static_cast<int32_t>(id));
    edges_.push_back({static_cast<int32_t>(head), capacity});
    adjacency_[head].push_back(static_cast<int32_t>(id + 1));
    edges_.push_back({static_cast<int32_t>(tail), 0});
    return id;
  }

  int64_t residual(size_t edge_id) const noexcept {
    return edges_[edge_id].residual;
  }

  void close_edge(size_t edge_id) noexcept {
    edges_[edge_id].residual = 0;
    edges_[edge_id ^ 1].residual = 0;
  }

  int64_t max_flow(size_t source, size_t sink) {
    int64_t flow = 0;
    while (build_levels(source, sink)) {
      std::fill(cursors_.begin(), cursors_.end(), 0);
      while (const int64_t pushed = augment(source, sink)) {
        flow += pushed;
      }
    }
    return flow;
  }

 private:
  struct Edge {
    int32_t head;
    int64_t residual;
  };

  bool build_levels(size_t source, size_t sink) {
    std::fill(levels_.begin(), levels_.end(), -1);
    levels_[source] = 0;
    queue_.clear();
    queue_.push_back(static_cast<int32_t>(source));
    for (size_t at = 0; at < queue_.size(); ++at) {
      const auto node = static_cast<size_t>(queue_[at]);
      for (const int32_t edge_id : adjacency_[node]) {
        const Edge& edge = edges_[static_cast<size_t>(edge_id)];
        const auto head = static_cast<size_t>(edge.head);
        if (edge.residual > 0 && levels_[head] < 0) {
          levels_[head] = levels_[node] + 1;
          queue_.push_back(edge.head);
        }
      }
    }
    return levels_[sink] >= 0;
  }

  // One augmenting path per call within the current level graph; shared
  // edge cursors never rewind, so a whole blocking-flow phase stays O(V*E)
  int64_t augment(size_t source, size_t sink) {
    path_.clear();
    size_t node = source;
    while (node != sink) {
      bool advanced = false;
      while (cursors_[node] < adjacency_[node].size()) {
        const int32_t edge_id = adjacency_[node][cursors_[node]];
        const Edge& edge = edges_[static_cast<size_t>(edge_id)];
        if (edge.residual > 0 &&
            levels_[static_cast<size_t>(edge.head)] == levels_[node] + 1) {
          path_.push_back(edge_id);
          node = static_cast<size_t>(edge.head);
          advanced = true;
          break;
        }
        ++cursors_[node];
      }
      if (!advanced) {
        levels_[node] = -1;
        if (path_.empty()) {
          return 0;
        }
        node = static_cast<size_t>(
            edges_[static_cast<size_t>(path_.back()) ^ 1].head);
        path_.pop_back();
      }
    }
    int64_t bottleneck = std::numeric_limits<int64_t>::max();
    for (const int32_t edge_id : path_) {
      bottleneck =
          std::min(bottleneck, edges_[static_cast<size_t>(edge_id)].residual);
    }
    for (const int32_t edge_id : path_) {
      edges_[static_cast<size_t>(edge_id)].residual -= bottleneck;
      edges_[static_cast<size_t>(edge_id) ^ 1].residual += bottleneck;
    }
    return bottleneck;
  }

  std::vector<Edge> edges_;
  std::vector<std::vector<int32_t>> adjacency_;
  std::vector<int32_t> levels_;
  std::vector<size_t> cursors_;
  std::vector<int32_t> queue_;
  std::vector<int32_t> path_;
};

// Max-weight antichain over explicit lifetime rows via the min-flow
// construction above; `max_threads` caps the dominance-edge pass so nested
// solves stay serial. `work_budget` bounds that pass and the network it feeds.
int64_t max_antichain(const std::vector<std::span<const int64_t>>& start_rows,
                      const std::vector<std::span<const int64_t>>& end_rows,
                      const std::vector<int64_t>& weights, size_t d,
                      bool serial, std::optional<uint64_t> work_budget) {
  const size_t g = weights.size();
  if (g == 0) {
    return 0;
  }
  const DedupedRows starts = dedupe_rows(start_rows, d);
  const DedupedRows ends = dedupe_rows(end_rows, d);
  const size_t k = starts.count();
  const size_t m = ends.count();
  // The flow network's up-to-k*m arcs dwarf the comparisons counted here.
  // Callers price that into tighter default budgets, never into the value
  // passed, so this gate and the linearize prelude's agree.
  const uint64_t needed = static_cast<uint64_t>(k) * m * d;
  if (work_budget && needed > *work_budget) {
    throw std::runtime_error(
        "Antichain flow work exceeds work_budget; pass work_budget=" +
        std::to_string(needed) +
        " to admit this instance, or None for no bound");
  }

  // Dominance edges end row -> start row, pruned to the suffix with
  // start[0] >= end[0] (rows ascend lexicographically on component 0)
  const unsigned num_threads = serial ? 1U : parallel_threads(m);
  std::vector<std::vector<int32_t>> dominated(m);
  for_each_row_block(m, num_threads, [&](size_t r) {
    const int64_t* end_row = ends.row(r);
    for (size_t q = starts.prefix_lt(end_row[0]); q < k; ++q) {
      if (dominates(end_row, starts.row(q), d)) {
        dominated[r].push_back(static_cast<int32_t>(q));
      }
    }
  });

  // Total weight caps every arc: no flow can usefully exceed it
  const int64_t total = std::reduce(weights.begin(), weights.end());
  const auto in_node = [](size_t i) { return 2 * i; };
  const auto out_node = [](size_t i) { return 2 * i + 1; };
  const size_t end_base = 2 * g;
  const size_t start_base = end_base + m;
  const size_t source = start_base + k;
  const size_t sink = source + 1;
  const size_t feas_source = sink + 1;
  const size_t feas_sink = feas_source + 1;

  Dinic network(feas_sink + 1);
  for (size_t i = 0; i < g; ++i) {
    network.add_edge(source, in_node(i), total);
    network.add_edge(out_node(i), sink, total);
    // Lower bound w_i on the split arc: unbounded residual arc plus the
    // standard super-source/super-sink demand arcs
    network.add_edge(in_node(i), out_node(i), total);
    network.add_edge(feas_source, out_node(i), weights[i]);
    network.add_edge(in_node(i), feas_sink, weights[i]);
    network.add_edge(out_node(i), end_base + static_cast<size_t>(ends.group[i]),
                     total);
    network.add_edge(start_base + static_cast<size_t>(starts.group[i]),
                     in_node(i), total);
  }
  for (size_t r = 0; r < m; ++r) {
    for (const int32_t q : dominated[r]) {
      network.add_edge(end_base + r, start_base + static_cast<size_t>(q),
                       total);
    }
  }
  const size_t circulation = network.add_edge(sink, source, total);

  if (network.max_flow(feas_source, feas_sink) != total) {
    throw std::runtime_error("lower-bound feasibility flow must saturate");
  }
  const int64_t feasible = total - network.residual(circulation);
  network.close_edge(circulation);
  return feasible - network.max_flow(sink, source);
}

}  // namespace

int64_t antichain_pressure(const std::vector<Allocation>& allocations,
                           std::optional<uint64_t> work_budget) {
  if (allocations.empty()) {
    return 0;
  }
  const size_t d = checked_dim(allocations);
  // Sizes are summed into int64 sweep deltas and flow arc capacities;
  // overflow would be UB, so refuse up front.
  check_total_size(allocations, std::numeric_limits<int64_t>::max());
  // Interval orders (all-scalar included) realize the antichain as the
  // scalar sweep peak: pairwise-overlapping intervals share a common point,
  // and linearization preserves the conflict relation exactly.
  if (const auto times = linearize_times(allocations, work_budget)) {
    std::vector<int64_t> weights(allocations.size());
    std::ranges::transform(allocations, weights.begin(), &Allocation::size);
    return interval_peak(*times, weights);
  }

  // Identical lifetimes merge into one weighted node (see the file header)
  const LifetimeGroups groups = group_lifetimes(allocations);
  return max_antichain(groups.starts, groups.ends, groups.weights, d,
                       /*serial=*/false, work_budget);
}

std::vector<int64_t> antichain_pressure_per_allocation(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget) {
  const size_t n = allocations.size();
  if (n == 0) {
    return {};
  }
  const size_t d = checked_dim(allocations);
  check_total_size(allocations, std::numeric_limits<int64_t>::max());
  // Interval orders: every clique through an allocation shares a time point
  // inside its own lifetime (Helly), so the pinned antichain is the window
  // peak of the linearized sweep.
  if (const auto times = linearize_times(allocations, work_budget)) {
    std::vector<int64_t> weights(n);
    std::ranges::transform(allocations, weights.begin(), &Allocation::size);
    return interval_peaks(*times, weights);
  }

  // Identical lifetimes share one pinned antichain, so solve per group; an
  // antichain through a group is the group plus an antichain among its
  // conflict neighbors, so each solve restricts to the neighborhood.
  const LifetimeGroups groups = group_lifetimes(allocations);
  const size_t g = groups.count();
  const ConflictSweep sweep(groups.starts, groups.ends, d);
  const CsrAdjacency adj = sweep.adjacency(parallel_threads(g));

  // One flow per group dwarfs thread startup, so parallelize from 2 rows up
  // and schedule single rows. Cap the workers: each solve materializes its
  // own flow network, so peak memory scales with workers, not cores.
  constexpr unsigned kMaxFlowThreads = 8;
  const unsigned num_threads =
      std::min({kMaxFlowThreads, max_threads(), static_cast<unsigned>(g)});
  std::vector<int64_t> pinned(g);
  for_each_row_block(
      g, num_threads,
      [&](size_t i) {
        const auto begin = static_cast<size_t>(adj.offsets[i]);
        const auto end = static_cast<size_t>(adj.offsets[i + 1]);
        std::vector<std::span<const int64_t>> starts;
        std::vector<std::span<const int64_t>> ends;
        std::vector<int64_t> weights;
        starts.reserve(end - begin);
        ends.reserve(end - begin);
        weights.reserve(end - begin);
        for (size_t e = begin; e < end; ++e) {
          const auto j = static_cast<size_t>(adj.neighbors[e]);
          starts.push_back(groups.starts[j]);
          ends.push_back(groups.ends[j]);
          weights.push_back(groups.weights[j]);
        }
        pinned[i] =
            groups.weights[i] + max_antichain(starts, ends, weights, d,
                                              /*serial=*/true, work_budget);
      },
      1);

  std::vector<int64_t> peaks(n);
  for (size_t i = 0; i < n; ++i) {
    peaks[i] = pinned[static_cast<size_t>(groups.group[i])];
  }
  return peaks;
}

}  // namespace omnimalloc
