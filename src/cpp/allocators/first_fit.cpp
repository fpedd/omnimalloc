//
// SPDX-License-Identifier: Apache-2.0
//

#include "first_fit.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <future>
#include <limits>
#include <numeric>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>

namespace omnimalloc {

void check_total_size(const std::vector<Allocation>& allocations,
                      int64_t limit) {
  int64_t total_size = 0;
  for (const Allocation& a : allocations) {
    if (a.size() > limit - total_size) {
      throw std::overflow_error("Total allocation size exceeds int64 range");
    }
    total_size += a.size();
  }
}

namespace {

// Sweep line over the single timeline; valid only for all-scalar input.
OverlapIndices scalar_overlap_indices(
    const std::vector<Allocation>& allocations) {
  std::vector<std::tuple<int64_t, bool, size_t>> events;
  events.reserve(allocations.size() * 2);
  for (size_t i = 0; i < allocations.size(); ++i) {
    events.emplace_back(allocations[i].start(), true, i);
    events.emplace_back(allocations[i].end(), false, i);
  }

  // Sort events by time; ends sort before starts at equal times, matching the
  // half-open interval semantics of Allocation::overlaps_temporally
  std::sort(events.begin(), events.end());

  OverlapIndices indices(allocations.size());
  std::vector<size_t> active;
  for (const auto& [time, is_start, idx] : events) {
    if (is_start) {
      // Current allocation overlaps with all currently active allocations
      for (size_t active_idx : active) {
        indices[idx].push_back(active_idx);
        indices[active_idx].push_back(idx);
      }
      active.push_back(idx);
    } else {
      active.erase(std::find(active.begin(), active.end(), idx));
    }
  }

  return indices;
}

// Component spans of all starts and ends, validated to one dimension and
// cached up front to keep the quadratic conflict loops branch-free.
struct ClockSpans {
  std::vector<std::span<const int64_t>> starts;
  std::vector<std::span<const int64_t>> ends;
};

ClockSpans cache_clock_spans(const std::vector<Allocation>& allocations) {
  ClockSpans spans;
  spans.starts.reserve(allocations.size());
  spans.ends.reserve(allocations.size());
  if (allocations.empty()) {
    return spans;
  }
  const size_t dim = allocations.front().dim();
  for (const auto& alloc : allocations) {
    if (alloc.dim() != dim) {
      throw std::invalid_argument(
          "clock dimension mismatch: " + std::to_string(dim) + " vs " +
          std::to_string(alloc.dim()));
    }
    spans.starts.push_back(alloc.start_vec());
    spans.ends.push_back(alloc.end_vec());
  }
  return spans;
}

// Parallelism kicks in only where work dwarfs thread startup cost
constexpr size_t kMinParallel = 512;

unsigned sweep_threads(size_t n) {
  if (n < kMinParallel) {
    return 1;
  }
  return std::min(8u, std::max(1u, std::thread::hardware_concurrency()));
}

// Dynamic row blocks: per-row costs vary wildly under the cutoff pruning, so
// static partitioning would leave threads idle
template <typename RowBody>
void for_each_row_block(size_t n, unsigned num_threads, RowBody&& row_body) {
  if (num_threads <= 1) {
    for (size_t row = 0; row < n; ++row) {
      row_body(row);
    }
    return;
  }
  constexpr size_t kBlock = 32;
  std::atomic<size_t> next{0};
  const auto worker = [&] {
    while (true) {
      const size_t begin = next.fetch_add(1) * kBlock;
      if (begin >= n) {
        return;
      }
      const size_t end = std::min(n, begin + kBlock);
      for (size_t row = begin; row < end; ++row) {
        row_body(row);
      }
    }
  };
  std::vector<std::future<void>> futures;
  futures.reserve(num_threads - 1);
  for (unsigned t = 1; t < num_threads; ++t) {
    futures.push_back(std::async(std::launch::async, worker));
  }
  worker();
  for (auto& future : futures) {
    future.get();
  }
}

// Clocks regathered contiguously in min-start order. Once the smallest start
// component of row b reaches the largest end component of row a, every start
// component of b dominates every end component of a, so a happens-before b
// and the row scan for a stops there: the pruning makes the quadratic pair
// sweep output-sensitive on loosely coupled workloads (and a plain sweep
// line on scalar timelines).
struct SweepTable {
  size_t n = 0;
  size_t dim = 0;
  std::vector<int64_t> starts;     // n x dim, row-major, min-start order
  std::vector<int64_t> ends;       // n x dim, row-major, min-start order
  std::vector<int64_t> min_start;  // ascending
  std::vector<int64_t> cutoff;     // max end component per row
  std::vector<int32_t> original;   // sorted row -> input index
};

SweepTable build_sweep_table(const std::vector<Allocation>& allocations) {
  const auto [starts, ends] = cache_clock_spans(allocations);
  SweepTable table;
  table.n = allocations.size();
  table.dim = table.n == 0 ? 0 : starts.front().size();
  const size_t n = table.n;
  const size_t d = table.dim;
  std::vector<int64_t> lo(n, std::numeric_limits<int64_t>::max());
  std::vector<int64_t> hi(n, std::numeric_limits<int64_t>::min());
  for (size_t i = 0; i < n; ++i) {
    for (size_t t = 0; t < d; ++t) {
      lo[i] = std::min(lo[i], starts[i][t]);
      hi[i] = std::max(hi[i], ends[i][t]);
    }
  }
  table.original.resize(n);
  std::iota(table.original.begin(), table.original.end(), 0);
  std::stable_sort(
      table.original.begin(), table.original.end(), [&](int32_t a, int32_t b) {
        return lo[static_cast<size_t>(a)] < lo[static_cast<size_t>(b)];
      });
  table.starts.resize(n * d);
  table.ends.resize(n * d);
  table.min_start.resize(n);
  table.cutoff.resize(n);
  for (size_t row = 0; row < n; ++row) {
    const auto i = static_cast<size_t>(table.original[row]);
    std::ranges::copy(starts[i], table.starts.begin() + row * d);
    std::ranges::copy(ends[i], table.ends.begin() + row * d);
    table.min_start[row] = lo[i];
    table.cutoff[row] = hi[i];
  }
  return table;
}

// Pairwise happens-before conflict test, O(n^2 * T) worst case; no single
// timeline to sweep once lifetimes are vector clocks. Calls `on_pair(a, b)`
// with a < b for every conflicting pair of sorted rows; `on_pair` must be
// thread-safe when num_threads > 1.
template <typename OnPair>
void sweep_conflict_pairs(const SweepTable& table, unsigned num_threads,
                          OnPair&& on_pair) {
  const size_t n = table.n;
  const size_t d = table.dim;
  for_each_row_block(n, num_threads, [&](size_t a) {
    const int64_t cutoff = table.cutoff[a];
    for (size_t b = a + 1; b < n && table.min_start[b] < cutoff; ++b) {
      // Branchless over the d components; conflict = neither happens-before
      bool ab = false;
      bool ba = false;
      for (size_t t = 0; t < d; ++t) {
        ab |= table.ends[a * d + t] > table.starts[b * d + t];
        ba |= table.ends[b * d + t] > table.starts[a * d + t];
      }
      if (ab && ba) {
        on_pair(a, b);
      }
    }
  });
}

// Conflict adjacency in CSR form: index-based, no id hashing on hot paths.
// Row contents are deterministic as multisets; the order within a row is not
// (parallel fill), and no consumer depends on it.
struct CsrAdjacency {
  std::vector<int64_t> offsets;
  std::vector<int32_t> neighbors;
};

CsrAdjacency build_conflict_adjacency(
    const std::vector<Allocation>& allocations) {
  const SweepTable table = build_sweep_table(allocations);
  const size_t n = table.n;
  const unsigned num_threads = sweep_threads(n);
  std::vector<std::atomic<int64_t>> degree(n);
  sweep_conflict_pairs(table, num_threads, [&](size_t a, size_t b) {
    degree[static_cast<size_t>(table.original[a])].fetch_add(
        1, std::memory_order_relaxed);
    degree[static_cast<size_t>(table.original[b])].fetch_add(
        1, std::memory_order_relaxed);
  });
  CsrAdjacency adj;
  adj.offsets.resize(n + 1);
  adj.offsets[0] = 0;
  for (size_t i = 0; i < n; ++i) {
    adj.offsets[i + 1] =
        adj.offsets[i] + degree[i].load(std::memory_order_relaxed);
  }
  adj.neighbors.resize(static_cast<size_t>(adj.offsets[n]));
  std::vector<std::atomic<int64_t>> cursor(n);
  for (size_t i = 0; i < n; ++i) {
    cursor[i].store(adj.offsets[i], std::memory_order_relaxed);
  }
  sweep_conflict_pairs(table, num_threads, [&](size_t a, size_t b) {
    const auto i = static_cast<size_t>(table.original[a]);
    const auto j = static_cast<size_t>(table.original[b]);
    adj.neighbors[static_cast<size_t>(cursor[i].fetch_add(
        1, std::memory_order_relaxed))] = static_cast<int32_t>(j);
    adj.neighbors[static_cast<size_t>(cursor[j].fetch_add(
        1, std::memory_order_relaxed))] = static_cast<int32_t>(i);
  });
  return adj;
}

// Pairwise happens-before adjacency; the only option for vector clocks
OverlapIndices vector_overlap_indices(
    const std::vector<Allocation>& allocations) {
  const CsrAdjacency adj = build_conflict_adjacency(allocations);
  OverlapIndices indices(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    indices[i].assign(adj.neighbors.begin() + adj.offsets[i],
                      adj.neighbors.begin() + adj.offsets[i + 1]);
  }
  return indices;
}

}  // namespace

OverlapIndices compute_overlap_indices(
    const std::vector<Allocation>& allocations) {
  const bool all_scalar =
      std::ranges::all_of(allocations, &Allocation::is_scalar_time);
  return all_scalar ? scalar_overlap_indices(allocations)
                    : vector_overlap_indices(allocations);
}

TemporalOverlaps compute_temporal_overlaps(
    const std::vector<Allocation>& allocations) {
  TemporalOverlaps overlaps;
  const OverlapIndices indices = compute_overlap_indices(allocations);
  for (size_t i = 0; i < allocations.size(); ++i) {
    for (size_t j : indices[i]) {
      overlaps[allocations[i].id()].insert(allocations[j].id());
    }
  }
  return overlaps;
}

std::vector<int64_t> compute_conflict_degrees(
    const std::vector<Allocation>& allocations) {
  const SweepTable table = build_sweep_table(allocations);
  std::vector<std::atomic<int64_t>> degree(table.n);
  sweep_conflict_pairs(table, sweep_threads(table.n), [&](size_t a, size_t b) {
    degree[static_cast<size_t>(table.original[a])].fetch_add(
        1, std::memory_order_relaxed);
    degree[static_cast<size_t>(table.original[b])].fetch_add(
        1, std::memory_order_relaxed);
  });
  std::vector<int64_t> degrees(table.n);
  for (size_t i = 0; i < table.n; ++i) {
    degrees[i] = degree[i].load(std::memory_order_relaxed);
  }
  return degrees;
}

namespace {

// Occupied span of a placed allocation
struct Interval {
  int64_t lo;
  int64_t hi;
};

// LSD radix sort by lo (hi rides along as payload; equal-lo order is
// irrelevant to the gap scan). The comparison sort dominated first-fit at
// scale, radix is ~5x cheaper; pass count scales with the actual offset
// magnitude, so no assumption on the offset range.
void sort_intervals_by_lo(std::vector<Interval>& intervals,
                          std::vector<Interval>& scratch) {
  const size_t m = intervals.size();
  if (m < 128) {
    std::sort(intervals.begin(), intervals.end(),
              [](const Interval& x, const Interval& y) { return x.lo < y.lo; });
    return;
  }
  uint64_t max_key = 0;
  for (const Interval& v : intervals) {
    max_key = std::max(max_key, static_cast<uint64_t>(v.lo));
  }
  constexpr int kDigitBits = 11;
  constexpr size_t kBuckets = size_t{1} << kDigitBits;
  scratch.resize(m);
  Interval* src = intervals.data();
  Interval* dst = scratch.data();
  int shift = 0;
  while ((max_key >> shift) != 0) {
    uint32_t count[kBuckets] = {};
    for (size_t i = 0; i < m; ++i) {
      ++count[(static_cast<uint64_t>(src[i].lo) >> shift) & (kBuckets - 1)];
    }
    uint32_t running = 0;
    for (size_t b = 0; b < kBuckets; ++b) {
      const uint32_t c = count[b];
      count[b] = running;
      running += c;
    }
    for (size_t i = 0; i < m; ++i) {
      dst[count[(static_cast<uint64_t>(src[i].lo) >> shift) &
                (kBuckets - 1)]++] = src[i];
    }
    std::swap(src, dst);
    shift += kDigitBits;
  }
  if (src != intervals.data()) {
    std::memcpy(intervals.data(), src, m * sizeof(Interval));
  }
}

// First-fit offsets for the allocations taken in `order`; same gap scan as
// `find_best_offset`, but over CSR neighbor indices instead of placed scans
std::vector<int64_t> place_order(const CsrAdjacency& adj,
                                 const std::vector<int64_t>& sizes,
                                 const std::vector<int32_t>& order) {
  constexpr Interval kUnplaced{-1, -1};
  std::vector<int64_t> offsets(sizes.size(), -1);
  std::vector<Interval> placed(sizes.size(), kUnplaced);
  std::vector<Interval> intervals;
  std::vector<Interval> scratch;
  for (const int32_t idx : order) {
    intervals.clear();
    for (int64_t e = adj.offsets[idx]; e < adj.offsets[idx + 1]; ++e) {
      const Interval span =
          placed[static_cast<size_t>(adj.neighbors[static_cast<size_t>(e)])];
      if (span.lo >= 0) {
        intervals.push_back(span);
      }
    }
    sort_intervals_by_lo(intervals, scratch);
    int64_t best = 0;
    for (const auto& [lo, hi] : intervals) {
      if (lo - best >= sizes[idx]) {
        break;
      }
      best = std::max(best, hi);
    }
    offsets[idx] = best;
    placed[static_cast<size_t>(idx)] = {best, best + sizes[idx]};
  }
  return offsets;
}

// The sort orders to place: the selected greedy_by_* order, or all seven for
// kAll, mirroring the greedy_by_* allocators over one shared adjacency
std::vector<std::vector<int32_t>> greedy_orders(
    GreedyOrder order, const std::vector<Allocation>& allocations,
    const CsrAdjacency& adj, const std::vector<int64_t>& sizes) {
  const size_t n = allocations.size();
  std::vector<int64_t> durations(n);
  for (size_t i = 0; i < n; ++i) {
    durations[i] = allocations[i].duration();
  }
  const auto degree = [&](int32_t i) {
    return adj.offsets[i + 1] - adj.offsets[i];
  };
  std::vector<int32_t> base(n);
  std::iota(base.begin(), base.end(), 0);
  const auto sorted_by = [&](auto&& less) {
    std::vector<int32_t> result = base;
    std::stable_sort(result.begin(), result.end(), less);
    return result;
  };
  const auto make = [&](GreedyOrder which) -> std::vector<int32_t> {
    switch (which) {
      case GreedyOrder::kSize:
        return sorted_by(
            [&](int32_t a, int32_t b) { return sizes[a] > sizes[b]; });
      case GreedyOrder::kDuration:
        return sorted_by(
            [&](int32_t a, int32_t b) { return durations[a] > durations[b]; });
      case GreedyOrder::kArea:
        return sorted_by([&](int32_t a, int32_t b) {
          return sizes[a] * durations[a] > sizes[b] * durations[b];
        });
      case GreedyOrder::kConflict:
        return sorted_by([&](int32_t a, int32_t b) {
          return std::pair(degree(a), sizes[a]) >
                 std::pair(degree(b), sizes[b]);
        });
      case GreedyOrder::kConflictSize:
        return sorted_by([&](int32_t a, int32_t b) {
          return std::pair(degree(a) * sizes[a], sizes[a]) >
                 std::pair(degree(b) * sizes[b], sizes[b]);
        });
      case GreedyOrder::kStart:
        return sorted_by([&](int32_t a, int32_t b) {
          const auto sa = allocations[a].start_vec();
          const auto sb = allocations[b].start_vec();
          const auto cmp = std::lexicographical_compare_three_way(
              sa.begin(), sa.end(), sb.begin(), sb.end());
          if (cmp != 0) {
            return cmp < 0;
          }
          return sizes[a] > sizes[b];
        });
      default:
        return base;
    }
  };
  if (order != GreedyOrder::kAll) {
    return {make(order)};
  }
  std::vector<std::vector<int32_t>> orders;
  orders.reserve(7);
  for (const GreedyOrder which :
       {GreedyOrder::kInput, GreedyOrder::kSize, GreedyOrder::kDuration,
        GreedyOrder::kArea, GreedyOrder::kConflict, GreedyOrder::kConflictSize,
        GreedyOrder::kStart}) {
    orders.push_back(make(which));
  }
  return orders;
}

}  // namespace

std::vector<int64_t> compute_allocation_peaks(
    const std::vector<Allocation>& allocations, GreedyOrder order) {
  const size_t n = allocations.size();
  const CsrAdjacency adj = build_conflict_adjacency(allocations);
  std::vector<int64_t> sizes(n);
  for (size_t i = 0; i < n; ++i) {
    sizes[i] = allocations[i].size();
  }
  const auto orders = greedy_orders(order, allocations, adj, sizes);

  // Placements are independent given the shared adjacency; spawning threads
  // only pays off once the placements dwarf the thread startup cost
  std::vector<std::vector<int64_t>> placements(orders.size());
  if (n < kMinParallel || orders.size() == 1) {
    for (size_t v = 0; v < orders.size(); ++v) {
      placements[v] = place_order(adj, sizes, orders[v]);
    }
  } else {
    std::vector<std::future<std::vector<int64_t>>> futures;
    futures.reserve(orders.size() - 1);
    for (size_t v = 1; v < orders.size(); ++v) {
      futures.push_back(std::async(std::launch::async, place_order,
                                   std::cref(adj), std::cref(sizes),
                                   std::cref(orders[v])));
    }
    placements[0] = place_order(adj, sizes, orders[0]);
    for (size_t v = 1; v < orders.size(); ++v) {
      placements[v] = futures[v - 1].get();
    }
  }

  const std::vector<int64_t>* best = nullptr;
  int64_t best_peak = std::numeric_limits<int64_t>::max();
  for (const auto& offsets : placements) {
    int64_t peak = 0;
    for (size_t i = 0; i < n; ++i) {
      peak = std::max(peak, offsets[i] + sizes[i]);
    }
    if (peak < best_peak) {
      best_peak = peak;
      best = &offsets;
    }
  }

  std::vector<int64_t> heights(n);
  for (size_t i = 0; i < n; ++i) {
    heights[i] = (*best)[i] + sizes[i];
  }
  std::vector<int64_t> peaks(n);
  for (size_t i = 0; i < n; ++i) {
    int64_t sum = sizes[i];
    int64_t top = heights[i];
    for (int64_t e = adj.offsets[i]; e < adj.offsets[i + 1]; ++e) {
      const int32_t j = adj.neighbors[static_cast<size_t>(e)];
      sum += sizes[j];
      top = std::max(top, heights[j]);
    }
    peaks[i] = std::min(sum, top);
  }
  return peaks;
}

void gather_spans(const std::vector<size_t>& neighbors,
                  const std::vector<std::optional<int64_t>>& offsets,
                  const std::vector<Allocation>& allocations,
                  std::vector<std::pair<int64_t, int64_t>>& spans) {
  spans.clear();
  for (size_t j : neighbors) {
    if (offsets[j].has_value()) {
      spans.emplace_back(*offsets[j], *offsets[j] + allocations[j].size());
    }
  }
  std::sort(spans.begin(), spans.end());
}

int64_t first_fit_offset(
    int64_t size, const std::vector<std::pair<int64_t, int64_t>>& spans) {
  int64_t best_offset = 0;
  for (const auto& [offset, end] : spans) {
    if (offset - best_offset >= size) {
      break;  // Found a fitting gap
    }
    best_offset = std::max(best_offset, end);
  }
  return best_offset;
}

std::vector<Allocation> first_fit_place_indexed(
    const std::vector<Allocation>& allocations, const OverlapIndices& indices) {
  // Lambda rather than the function pointer so the placement loop inlines
  // the offset scan instead of an indirect call per allocation
  return place_indexed(allocations, indices,
                       [](int64_t size, const auto& spans) {
                         return first_fit_offset(size, spans);
                       });
}

std::vector<Allocation> first_fit_place(
    const std::vector<Allocation>& allocations,
    const TemporalOverlaps& overlaps) {
  // Translate the id-keyed map into index adjacency once, so placement only
  // visits each allocation's neighbors instead of everything placed so far
  std::unordered_map<IdType, std::vector<size_t>, IdTypeHash> by_id;
  for (size_t i = 0; i < allocations.size(); ++i) {
    by_id[allocations[i].id()].push_back(i);
  }

  OverlapIndices indices(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    auto it = overlaps.find(allocations[i].id());
    if (it == overlaps.end()) {
      continue;
    }
    for (const IdType& other_id : it->second) {
      auto jt = by_id.find(other_id);
      if (jt == by_id.end()) {
        continue;
      }
      for (size_t j : jt->second) {
        if (j != i) {
          indices[i].push_back(j);
        }
      }
    }
  }

  return first_fit_place_indexed(allocations, indices);
}

FirstFitPlacer::FirstFitPlacer(std::vector<Allocation> allocations)
    : allocations_(std::move(allocations)),
      overlaps_(compute_temporal_overlaps(allocations_)),
      indices_(compute_overlap_indices(allocations_)) {
  check_total_size(allocations_);
}

std::vector<std::optional<int64_t>> FirstFitPlacer::place_offsets(
    const std::vector<size_t>& order) const {
  std::vector<std::optional<int64_t>> offsets(allocations_.size());
  std::vector<std::pair<int64_t, int64_t>> spans;
  for (size_t idx : order) {
    const Allocation& alloc = allocations_.at(idx);
    gather_spans(indices_[idx], offsets, allocations_, spans);
    offsets[idx] = first_fit_offset(alloc.size(), spans);
  }
  return offsets;
}

std::vector<Allocation> FirstFitPlacer::place(
    const std::vector<size_t>& order) const {
  const auto offsets = place_offsets(order);
  std::vector<Allocation> placed;
  placed.reserve(order.size());
  for (size_t idx : order) {
    placed.push_back(allocations_[idx].with_offset(*offsets[idx]));
  }
  return placed;
}

int64_t FirstFitPlacer::evaluate(const std::vector<size_t>& order) const {
  const auto offsets = place_offsets(order);
  int64_t peak = 0;
  for (size_t idx : order) {
    peak = std::max(peak, *offsets[idx] + allocations_[idx].size());
  }
  return peak;
}

}  // namespace omnimalloc
