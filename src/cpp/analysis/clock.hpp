//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/parallel.hpp"
#include "primitives/allocation.hpp"

// Shared clock-row utilities for the exact vector-time analyses
// (linearize, antichain, closure) and the conflict-graph consumers: row
// deduplication, componentwise dominance, lifetime grouping, the scalar
// sweep peaks, and the pruned pairwise conflict sweep.

namespace omnimalloc {

// Shared clock dimension (1 when empty); throws on mixed dimensions.
inline size_t checked_dim(const std::vector<Allocation>& allocations) {
  const size_t dim = allocations.empty() ? 1 : allocations.front().dim();
  for (const Allocation& alloc : allocations) {
    if (alloc.dim() != dim) {
      throw std::invalid_argument(
          "allocations must share one clock dimension, got " +
          std::to_string(dim) + " and " + std::to_string(alloc.dim()));
    }
  }
  return dim;
}

// Component spans of all starts and ends plus the shared clock dimension,
// validated and gathered up front to keep the conflict and dominance loops
// branch-free.
struct ClockSpans {
  std::vector<std::span<const int64_t>> starts;
  std::vector<std::span<const int64_t>> ends;
  size_t dim = 1;
};

inline ClockSpans gather_clock_spans(
    const std::vector<Allocation>& allocations) {
  ClockSpans spans;
  spans.dim = checked_dim(allocations);
  spans.starts.reserve(allocations.size());
  spans.ends.reserve(allocations.size());
  for (const Allocation& alloc : allocations) {
    spans.starts.push_back(alloc.start_vec());
    spans.ends.push_back(alloc.end_vec());
  }
  return spans;
}

// Distinct clock rows in lexicographic order (so component 0 ascends), with
// per-row multiplicities and each input's row index.
struct DedupedRows {
  size_t dim = 0;
  std::vector<int64_t> rows;     // count x dim, row-major
  std::vector<int64_t> weights;  // multiplicity per row
  std::vector<int32_t> group;    // input index -> row index

  size_t count() const noexcept { return weights.size(); }
  const int64_t* row(size_t r) const noexcept { return rows.data() + r * dim; }
};

inline DedupedRows dedupe_rows(
    const std::vector<std::span<const int64_t>>& inputs, size_t dim) {
  const size_t n = inputs.size();
  std::vector<int32_t> order(n);
  std::iota(order.begin(), order.end(), 0);
  std::sort(order.begin(), order.end(), [&](int32_t a, int32_t b) {
    return std::ranges::lexicographical_compare(inputs[static_cast<size_t>(a)],
                                                inputs[static_cast<size_t>(b)]);
  });
  DedupedRows out;
  out.dim = dim;
  out.group.resize(n);
  for (size_t pos = 0; pos < n; ++pos) {
    const auto i = static_cast<size_t>(order[pos]);
    if (pos == 0 ||
        !std::ranges::equal(inputs[static_cast<size_t>(order[pos - 1])],
                            inputs[i])) {
      out.rows.insert(out.rows.end(), inputs[i].begin(), inputs[i].end());
      out.weights.push_back(0);
    }
    out.group[i] = static_cast<int32_t>(out.weights.size() - 1);
    ++out.weights.back();
  }
  return out;
}

inline bool dominates(const int64_t* end, const int64_t* start,
                      size_t dim) noexcept {
  return happens_before({end, dim}, {start, dim});
}

// Allocations grouped by identical (start, end) clock pairs: one
// representative lifetime per group with the group's summed size. Identical
// lifetimes are mutually concurrent and relate identically to everything
// else, so exact pressure computations may treat each group as one unit.
struct LifetimeGroups {
  std::vector<std::span<const int64_t>> starts;  // representative per group
  std::vector<std::span<const int64_t>> ends;
  std::vector<int64_t> weights;  // summed sizes
  std::vector<int32_t> group;    // input index -> group index

  size_t count() const noexcept { return weights.size(); }
};

inline LifetimeGroups group_lifetimes(
    const std::vector<Allocation>& allocations) {
  const size_t n = allocations.size();
  std::vector<int32_t> order(n);
  std::iota(order.begin(), order.end(), 0);
  const auto lifetime = [&](int32_t i) {
    const Allocation& alloc = allocations[static_cast<size_t>(i)];
    return std::pair{alloc.start_vec(), alloc.end_vec()};
  };
  const auto lifetime_less = [&](int32_t a, int32_t b) {
    const auto [start_a, end_a] = lifetime(a);
    const auto [start_b, end_b] = lifetime(b);
    const auto cmp = std::lexicographical_compare_three_way(
        start_a.begin(), start_a.end(), start_b.begin(), start_b.end());
    if (cmp != 0) {
      return cmp < 0;
    }
    return std::ranges::lexicographical_compare(end_a, end_b);
  };
  std::sort(order.begin(), order.end(), lifetime_less);
  LifetimeGroups out;
  out.group.resize(n);
  for (size_t pos = 0; pos < n; ++pos) {
    const auto [start, end] = lifetime(order[pos]);
    if (pos == 0 || lifetime_less(order[pos - 1], order[pos])) {
      out.starts.push_back(start);
      out.ends.push_back(end);
      out.weights.push_back(0);
    }
    out.group[static_cast<size_t>(order[pos])] =
        static_cast<int32_t>(out.weights.size() - 1);
    out.weights.back() += allocations[static_cast<size_t>(order[pos])].size();
  }
  return out;
}

// Sweep peak of weighted [start, end) intervals: the exact maximum
// concurrently live weight (coincident ends release before starts claim).
inline int64_t interval_peak(
    const std::vector<std::pair<int64_t, int64_t>>& times,
    const std::vector<int64_t>& weights) {
  std::vector<std::pair<int64_t, int64_t>> events;
  events.reserve(2 * times.size());
  for (size_t i = 0; i < times.size(); ++i) {
    events.emplace_back(times[i].first, weights[i]);
    events.emplace_back(times[i].second, -weights[i]);
  }
  std::sort(events.begin(), events.end());
  int64_t peak = 0;
  int64_t current = 0;
  for (const auto& [time, delta] : events) {
    current += delta;
    peak = std::max(peak, current);
  }
  return peak;
}

// Iterative segment tree over a fixed array: O(n) memory, O(log n) max
// queries on half-open index ranges.
class MaxSegtree {
 public:
  explicit MaxSegtree(const std::vector<int64_t>& values)
      : size_(std::bit_ceil(std::max<size_t>(values.size(), 1))),
        tree_(2 * size_, std::numeric_limits<int64_t>::min()) {
    std::ranges::copy(values, tree_.begin() + static_cast<ptrdiff_t>(size_));
    for (size_t node = size_ - 1; node > 0; --node) {
      tree_[node] = std::max(tree_[2 * node], tree_[2 * node + 1]);
    }
  }

  int64_t max(size_t lo, size_t hi) const noexcept {  // over [lo, hi)
    int64_t best = std::numeric_limits<int64_t>::min();
    for (lo += size_, hi += size_; lo < hi; lo /= 2, hi /= 2) {
      if (lo % 2 == 1) {
        best = std::max(best, tree_[lo++]);
      }
      if (hi % 2 == 1) {
        best = std::max(best, tree_[--hi]);
      }
    }
    return best;
  }

 private:
  size_t size_;
  std::vector<int64_t> tree_;
};

// Sorted distinct interval bounds: slot j spans [bounds[j], bounds[j + 1]).
inline std::vector<int64_t> slot_bounds(
    const std::vector<std::pair<int64_t, int64_t>>& times) {
  std::vector<int64_t> bounds;
  bounds.reserve(2 * times.size());
  for (const auto& [start, end] : times) {
    bounds.push_back(start);
    bounds.push_back(end);
  }
  std::ranges::sort(bounds);
  bounds.erase(std::unique(bounds.begin(), bounds.end()), bounds.end());
  return bounds;
}

inline size_t slot_index(const std::vector<int64_t>& bounds,
                         int64_t time) noexcept {
  return static_cast<size_t>(std::ranges::lower_bound(bounds, time) -
                             bounds.begin());
}

// Per-interval window peaks: for each [start, end), the maximum
// concurrently live weight at any point inside the window (every interval
// is live in its own window, so each peak is at least its own weight).
inline std::vector<int64_t> interval_peaks(
    const std::vector<std::pair<int64_t, int64_t>>& times,
    const std::vector<int64_t>& weights) {
  const std::vector<int64_t> bounds = slot_bounds(times);
  std::vector<int64_t> pressure(bounds.size(), 0);
  for (size_t i = 0; i < times.size(); ++i) {
    pressure[slot_index(bounds, times[i].first)] += weights[i];
    pressure[slot_index(bounds, times[i].second)] -= weights[i];
  }
  std::partial_sum(pressure.begin(), pressure.end(), pressure.begin());
  const MaxSegtree live(pressure);
  std::vector<int64_t> peaks(times.size());
  for (size_t i = 0; i < times.size(); ++i) {
    peaks[i] = live.max(slot_index(bounds, times[i].first),
                        slot_index(bounds, times[i].second));
  }
  return peaks;
}

// Conflict adjacency in CSR form: index-based, no id hashing on hot paths.
// Row contents are deterministic as multisets; the order within a row is not
// (parallel fill), and no consumer depends on it.
struct CsrAdjacency {
  std::vector<int64_t> offsets;
  std::vector<int32_t> neighbors;
};

// Pairwise happens-before conflict sweep, O(n^2 * T) worst case; no single
// timeline to sweep once lifetimes are vector clocks. Clocks are regathered
// contiguously in min-start order: once the smallest start component of row
// b reaches the largest end component of row a, every start component of b
// dominates every end component of a, so a happens-before b and the row
// scan for a stops there. The pruning makes the quadratic pair sweep
// output-sensitive on loosely coupled workloads (and a plain sweep line on
// scalar timelines).
class ConflictSweep {
 public:
  ConflictSweep(const std::vector<std::span<const int64_t>>& starts,
                const std::vector<std::span<const int64_t>>& ends, size_t dim)
      : n_(starts.size()), dim_(dim) {
    std::vector<int64_t> lo(n_, std::numeric_limits<int64_t>::max());
    std::vector<int64_t> hi(n_, std::numeric_limits<int64_t>::min());
    for (size_t i = 0; i < n_; ++i) {
      for (size_t t = 0; t < dim_; ++t) {
        lo[i] = std::min(lo[i], starts[i][t]);
        hi[i] = std::max(hi[i], ends[i][t]);
      }
    }
    original_.resize(n_);
    std::iota(original_.begin(), original_.end(), 0);
    std::stable_sort(
        original_.begin(), original_.end(), [&](int32_t a, int32_t b) {
          return lo[static_cast<size_t>(a)] < lo[static_cast<size_t>(b)];
        });
    starts_.resize(n_ * dim_);
    ends_.resize(n_ * dim_);
    min_start_.resize(n_);
    cutoff_.resize(n_);
    for (size_t row = 0; row < n_; ++row) {
      const auto i = static_cast<size_t>(original_[row]);
      std::ranges::copy(starts[i],
                        starts_.begin() + static_cast<ptrdiff_t>(row * dim_));
      std::ranges::copy(ends[i],
                        ends_.begin() + static_cast<ptrdiff_t>(row * dim_));
      min_start_[row] = lo[i];
      cutoff_[row] = hi[i];
    }
  }

  size_t count() const noexcept { return n_; }

  // Work the pruned pair sweep will perform, in component comparisons: row
  // a scans until the ascending min-starts reach its cutoff, so each scan
  // length falls out of one binary search and no pair is touched.
  [[nodiscard]] uint64_t sweep_work() const noexcept {
    uint64_t pairs = 0;
    for (size_t a = 0; a < n_; ++a) {
      const auto begin = min_start_.begin() + static_cast<ptrdiff_t>(a) + 1;
      pairs += static_cast<uint64_t>(
          std::lower_bound(begin, min_start_.end(), cutoff_[a]) - begin);
    }
    return pairs * dim_;
  }

  // Calls `on_pair(i, j)` once per conflicting pair, in input indices;
  // `on_pair` must be thread-safe when num_threads > 1.
  template <typename OnPair>
  void for_each_pair(unsigned num_threads, OnPair&& on_pair) const {
    for_each_row_block(n_, num_threads, [&](size_t a) {
      const int64_t cutoff = cutoff_[a];
      for (size_t b = a + 1; b < n_ && min_start_[b] < cutoff; ++b) {
        // Branchless over the dim components; conflict = neither
        // happens-before
        bool ab = false;
        bool ba = false;
        for (size_t t = 0; t < dim_; ++t) {
          ab |= ends_[a * dim_ + t] > starts_[b * dim_ + t];
          ba |= ends_[b * dim_ + t] > starts_[a * dim_ + t];
        }
        if (ab && ba) {
          on_pair(static_cast<size_t>(original_[a]),
                  static_cast<size_t>(original_[b]));
        }
      }
    });
  }

  // CSR adjacency over two sweeps: count degrees, then fill through atomic
  // per-row cursors.
  CsrAdjacency adjacency(unsigned num_threads) const {
    std::vector<std::atomic<int64_t>> slots(n_);
    for_each_pair(num_threads, [&](size_t i, size_t j) {
      slots[i].fetch_add(1, std::memory_order_relaxed);
      slots[j].fetch_add(1, std::memory_order_relaxed);
    });
    CsrAdjacency adj;
    adj.offsets.resize(n_ + 1);
    adj.offsets[0] = 0;
    for (size_t i = 0; i < n_; ++i) {
      adj.offsets[i + 1] =
          adj.offsets[i] + slots[i].load(std::memory_order_relaxed);
      slots[i].store(adj.offsets[i], std::memory_order_relaxed);
    }
    adj.neighbors.resize(static_cast<size_t>(adj.offsets[n_]));
    for_each_pair(num_threads, [&](size_t i, size_t j) {
      adj.neighbors[static_cast<size_t>(slots[i].fetch_add(
          1, std::memory_order_relaxed))] = static_cast<int32_t>(j);
      adj.neighbors[static_cast<size_t>(slots[j].fetch_add(
          1, std::memory_order_relaxed))] = static_cast<int32_t>(i);
    });
    return adj;
  }

 private:
  size_t n_;
  size_t dim_;
  std::vector<int64_t> starts_;     // n x dim, row-major, min-start order
  std::vector<int64_t> ends_;       // n x dim, row-major, min-start order
  std::vector<int64_t> min_start_;  // ascending
  std::vector<int64_t> cutoff_;     // max end component per row
  std::vector<int32_t> original_;   // sorted row -> input index
};

}  // namespace omnimalloc
