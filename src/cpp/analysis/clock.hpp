//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <algorithm>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/parallel.hpp"
#include "primitives/allocation.hpp"

// Shared clock-row utilities for the exact vector-time analyses (linearize,
// antichain, closure) and the conflict-graph consumers: deduplication, column
// reduction, dominance, lifetime grouping, and the sweep peaks.

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

// Fold one clock component into a column fingerprint (boost's hash_combine).
// Fingerprints only prune the exact comparison below, so their quality
// bounds redundant work and never correctness.
inline uint64_t hash_component(uint64_t seed, int64_t value) noexcept {
  return seed ^ (static_cast<uint64_t>(value) + 0x9e3779b97f4a7c15ULL +
                 (seed << 6) + (seed >> 2));
}

// Collapse degenerate clock columns in place, returning the reduced rows'
// backing storage (empty when every column survives; it must outlive `spans`).
// A constant column never decides a dominance test and a duplicate repeats one.
[[nodiscard]] inline std::vector<int64_t> reduce_columns(ClockSpans& spans) {
  const size_t n = spans.starts.size();
  const size_t d = spans.dim;
  if (d == 1 || n == 0) {
    return {};
  }
  std::vector<uint64_t> fingerprint(d, 0);
  std::vector<char> constant(d, 1);
  const std::span<const int64_t> first = spans.starts[0];
  for (size_t i = 0; i < n; ++i) {
    const std::span<const int64_t> start = spans.starts[i];
    const std::span<const int64_t> end = spans.ends[i];
    for (size_t c = 0; c < d; ++c) {
      fingerprint[c] =
          hash_component(hash_component(fingerprint[c], start[c]), end[c]);
      if (start[c] != first[c] || end[c] != first[c]) {
        constant[c] = 0;
      }
    }
  }
  const auto column_equal = [&](size_t a, size_t b) {
    for (size_t i = 0; i < n; ++i) {
      if (spans.starts[i][a] != spans.starts[i][b] ||
          spans.ends[i][a] != spans.ends[i][b]) {
        return false;
      }
    }
    return true;
  };
  std::vector<size_t> keep;
  for (size_t c = 0; c < d; ++c) {
    if (constant[c] != 0) {
      continue;
    }
    const bool duplicate = std::ranges::any_of(keep, [&](size_t kc) {
      return fingerprint[kc] == fingerprint[c] && column_equal(kc, c);
    });
    if (!duplicate) {
      keep.push_back(c);
    }
  }
  // All-constant clocks would need start == end, which validation rejects
  assert(!keep.empty());
  if (keep.size() == d) {
    return {};
  }
  std::vector<int64_t> backing(2 * n * keep.size());
  for (size_t i = 0; i < n; ++i) {
    for (size_t c = 0; c < keep.size(); ++c) {
      backing[i * keep.size() + c] = spans.starts[i][keep[c]];
      backing[(n + i) * keep.size() + c] = spans.ends[i][keep[c]];
    }
  }
  for (size_t i = 0; i < n; ++i) {
    spans.starts[i] = {backing.data() + i * keep.size(), keep.size()};
    spans.ends[i] = {backing.data() + (n + i) * keep.size(), keep.size()};
  }
  spans.dim = keep.size();
  return backing;
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

  // Rows ascend lexicographically, so component 0 admits binary search:
  // number of rows with row[0] < v, and with row[0] <= v respectively.
  size_t prefix_lt(int64_t v) const noexcept {
    return *std::ranges::partition_point(
        std::views::iota(size_t{0}, count()),
        [&](size_t r) { return row(r)[0] < v; });
  }
  size_t prefix_leq(int64_t v) const noexcept {
    return *std::ranges::partition_point(
        std::views::iota(size_t{0}, count()),
        [&](size_t r) { return row(r)[0] <= v; });
  }
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
// representative lifetime per group with the group's summed size. Such
// lifetimes relate identically to everything else, so a group is one unit.
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

// Ceiling on the neighbor entries one adjacency may materialize, 4 bytes each.
// No work budget bounds the CSR: budgets count the sweep, and the sweep is what
// fills the rows. A guard against taking the host down, not a tuning knob.
inline constexpr uint64_t kMaxAdjacencyEntries = uint64_t{1} << 31;

// Pairwise happens-before conflict sweep, O(n^2 * T) worst case; vector clocks
// leave no single timeline. Min-start row order lets a's scan stop once b's
// smallest start passes a's largest end, keeping the sweep output-sensitive.
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
  size_t dim() const noexcept { return dim_; }

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
        // Conflict = neither happens-before; each dominance test exits on
        // its first violating component, so conflicting pairs resolve fast
        if (!dominates(&ends_[a * dim_], &starts_[b * dim_], dim_) &&
            !dominates(&ends_[b * dim_], &starts_[a * dim_], dim_)) {
          on_pair(static_cast<size_t>(original_[a]),
                  static_cast<size_t>(original_[b]));
        }
      }
    });
  }

  // Conflict count per input index. Scalar timelines skip the pair sweep: two
  // binary searches on the sorted bounds give everything started before a's
  // end minus everything ended by its start, less a itself (half-open).
  [[nodiscard]] std::vector<int64_t> degrees(unsigned num_threads) const {
    std::vector<int64_t> result(n_);
    if (dim_ == 1) {
      std::vector<int64_t> sorted_ends(cutoff_);
      std::ranges::sort(sorted_ends);
      for (size_t a = 0; a < n_; ++a) {
        const auto started = std::ranges::lower_bound(min_start_, cutoff_[a]) -
                             min_start_.begin();
        const auto ended =
            std::ranges::upper_bound(sorted_ends, min_start_[a]) -
            sorted_ends.begin();
        result[static_cast<size_t>(original_[a])] = started - ended - 1;
      }
      return result;
    }
    std::vector<std::atomic<int64_t>> counts(n_);
    for_each_pair(num_threads, [&](size_t i, size_t j) {
      counts[i].fetch_add(1, std::memory_order_relaxed);
      counts[j].fetch_add(1, std::memory_order_relaxed);
    });
    for (size_t i = 0; i < n_; ++i) {
      result[i] = counts[i].load(std::memory_order_relaxed);
    }
    return result;
  }

  // CSR adjacency: degrees first (binary searches on scalar timelines, a
  // counting sweep on vector clocks), then one fill sweep through atomic
  // per-row cursors. The prefix sum is exact, so the ceiling refuses early.
  [[nodiscard]] CsrAdjacency adjacency(
      unsigned num_threads, uint64_t max_entries = kMaxAdjacencyEntries) const {
    const std::vector<int64_t> degree = degrees(num_threads);
    CsrAdjacency adj;
    adj.offsets.resize(n_ + 1);
    adj.offsets[0] = 0;
    std::vector<std::atomic<int64_t>> slots(n_);
    for (size_t i = 0; i < n_; ++i) {
      adj.offsets[i + 1] = adj.offsets[i] + degree[i];
      slots[i].store(adj.offsets[i], std::memory_order_relaxed);
    }
    if (static_cast<uint64_t>(adj.offsets[n_]) > max_entries) {
      throw std::runtime_error(
          "Conflict adjacency needs " + std::to_string(adj.offsets[n_]) +
          " neighbor entries at 4 bytes each, over the " +
          std::to_string(max_entries) +
          " ceiling; the conflict relation is too dense to materialize");
    }
    adj.neighbors.resize(static_cast<size_t>(adj.offsets[n_]));
    for_each_pair(num_threads, [&](size_t i, size_t j) {
      adj.neighbors[static_cast<size_t>(slots[i].fetch_add(
          1, std::memory_order_relaxed))] = static_cast<int32_t>(j);
      adj.neighbors[static_cast<size_t>(slots[j].fetch_add(
          1, std::memory_order_relaxed))] = static_cast<int32_t>(i);
    });
    // dim==1 sizes rows by formula but fills by sweep; disagreement would
    // silently corrupt adjacent rows, so pin it where debug builds see it
#ifndef NDEBUG
    for (size_t i = 0; i < n_; ++i) {
      assert(slots[i].load(std::memory_order_relaxed) == adj.offsets[i + 1]);
    }
#endif
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
