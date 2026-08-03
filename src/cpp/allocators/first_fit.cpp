//
// SPDX-License-Identifier: Apache-2.0
//

#include "first_fit.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/parallel.hpp"

namespace omnimalloc {

namespace {

// Occupied (offset, end) span of a placed allocation, matching the span
// shape that `first_fit_offset` consumes
using Interval = std::pair<int64_t, int64_t>;

// LSD radix sort by offset (the end rides along as payload; equal-offset order
// is irrelevant to the gap scan). Replaces the comparison sort that dominated
// first-fit at scale; pass count scales with the actual offset magnitude.
void sort_intervals_by_lo(std::vector<Interval>& intervals,
                          std::vector<Interval>& scratch) {
  const size_t m = intervals.size();
  if (m < 128) {
    std::sort(intervals.begin(), intervals.end());
    return;
  }
  uint64_t max_key = 0;
  for (const Interval& v : intervals) {
    max_key = std::max(max_key, static_cast<uint64_t>(v.first));
  }
  constexpr int kDigitBits = 11;
  constexpr size_t kBuckets = size_t{1} << kDigitBits;
  scratch.resize(m);
  Interval* src = intervals.data();
  Interval* dst = scratch.data();
  int shift = 0;
  // shift < 64: shifting a uint64_t by >= 64 is UB, and the 55..63 digit
  // already covers every remaining bit
  while (shift < 64 && (max_key >> shift) != 0) {
    uint32_t count[kBuckets] = {};
    for (size_t i = 0; i < m; ++i) {
      ++count[(static_cast<uint64_t>(src[i].first) >> shift) & (kBuckets - 1)];
    }
    uint32_t running = 0;
    for (size_t b = 0; b < kBuckets; ++b) {
      const uint32_t c = count[b];
      count[b] = running;
      running += c;
    }
    for (size_t i = 0; i < m; ++i) {
      dst[count[(static_cast<uint64_t>(src[i].first) >> shift) &
                (kBuckets - 1)]++] = src[i];
    }
    std::swap(src, dst);
    shift += kDigitBits;
  }
  if (src != intervals.data()) {
    std::copy_n(src, m, intervals.data());
  }
}

// First-fit offsets for the allocations taken in `order`, gathering each
// allocation's placed CSR neighbors and reusing the shared gap scan. A
// non-negative `pins[i]` fixes i there, an obstacle before the first scan.
std::vector<int64_t> place_order(const CsrAdjacency& adj,
                                 const std::vector<int64_t>& sizes,
                                 const std::vector<int64_t>& pins,
                                 const std::vector<int32_t>& order) {
  constexpr Interval kUnplaced{-1, -1};
  std::vector<int64_t> offsets(sizes.size(), -1);
  std::vector<Interval> placed(sizes.size(), kUnplaced);
  for (size_t i = 0; i < sizes.size(); ++i) {
    if (pins[i] >= 0) {
      offsets[i] = pins[i];
      placed[i] = {pins[i], pins[i] + sizes[i]};
    }
  }
  std::vector<Interval> intervals;
  std::vector<Interval> scratch;
  for (const int32_t idx : order) {
    if (pins[static_cast<size_t>(idx)] >= 0) {
      continue;
    }
    intervals.clear();
    for (int64_t e = adj.offsets[idx]; e < adj.offsets[idx + 1]; ++e) {
      const Interval span =
          placed[static_cast<size_t>(adj.neighbors[static_cast<size_t>(e)])];
      if (span.first >= 0) {
        intervals.push_back(span);
      }
    }
    sort_intervals_by_lo(intervals, scratch);
    const int64_t best = first_fit_offset(sizes[idx], intervals);
    offsets[idx] = best;
    placed[static_cast<size_t>(idx)] = {best, best + sizes[idx]};
  }
  return offsets;
}

// Saturating product for the conflict x size sort key: a raw int64 product
// overflows (UB) on legal inputs; saturated ties at the extreme order as
// well as anything can (Allocation::area() saturates the same way).
int64_t saturating_product(int64_t a, int64_t b) noexcept {
  if (a > 0 && b > std::numeric_limits<int64_t>::max() / a) {
    return std::numeric_limits<int64_t>::max();
  }
  return a * b;
}

// Indices sorted stably by `less`, so equal keys keep input order (matching
// the greedy_by_* allocators' stable sorts)
template <typename Less>
std::vector<int32_t> sorted_by(const std::vector<int32_t>& base, Less&& less) {
  std::vector<int32_t> result = base;
  std::stable_sort(result.begin(), result.end(), less);
  return result;
}

// Start components in canonical lane order, row-major (n x d), for the one
// comparator that reads raw clock components: ordering lanes by their own
// contents stops arbitrary lane labelling from deciding the packing.
std::vector<int64_t> canonical_starts(const std::vector<Allocation>& times) {
  const size_t n = times.size();
  const size_t d = n == 0 ? 1 : times[0].dim();
  std::vector<size_t> lanes(d);
  std::iota(lanes.begin(), lanes.end(), 0);
  if (d > 1) {
    std::vector<uint64_t> fingerprint(d, 0);
    for (size_t i = 0; i < n; ++i) {
      const auto start = times[i].start_vec();
      const auto end = times[i].end_vec();
      for (size_t c = 0; c < d; ++c) {
        fingerprint[c] =
            hash_component(hash_component(fingerprint[c], start[c]), end[c]);
      }
    }
    const auto content_less = [&](size_t a, size_t b) {
      for (size_t i = 0; i < n; ++i) {
        const auto start = times[i].start_vec();
        const auto end = times[i].end_vec();
        if (start[a] != start[b]) {
          return start[a] < start[b];
        }
        if (end[a] != end[b]) {
          return end[a] < end[b];
        }
      }
      return false;
    };
    std::ranges::sort(lanes, [&](size_t a, size_t b) {
      return fingerprint[a] != fingerprint[b] ? fingerprint[a] < fingerprint[b]
                                              : content_less(a, b);
    });
  }
  std::vector<int64_t> rows(n * d);
  for (size_t i = 0; i < n; ++i) {
    const auto start = times[i].start_vec();
    for (size_t c = 0; c < d; ++c) {
      rows[i * d + c] = start[lanes[c]];
    }
  }
  return rows;
}

// The three orders one timeline contributes: greedy_by_duration,
// greedy_by_area and greedy_by_start. `times` is the input clocks or a
// surrogate linearization; all three are invariant under permuting the lanes.
std::array<std::vector<int32_t>, 3> time_orders(
    const std::vector<Allocation>& times, const std::vector<int64_t>& sizes,
    const std::vector<int32_t>& base) {
  const size_t n = times.size();
  std::vector<int64_t> durations(n);
  std::vector<int64_t> areas(n);
  std::ranges::transform(times, durations.begin(), &Allocation::duration);
  std::ranges::transform(times, areas.begin(), &Allocation::area);
  const std::vector<int64_t> starts = canonical_starts(times);
  const size_t d = n == 0 ? 1 : starts.size() / n;
  return {sorted_by(base,  // greedy_by_duration
                    [&](int32_t a, int32_t b) {
                      return durations[a] > durations[b];
                    }),
          sorted_by(base,  // greedy_by_area
                    [&](int32_t a, int32_t b) { return areas[a] > areas[b]; }),
          sorted_by(base, [&](int32_t a, int32_t b) {  // greedy_by_start
            const int64_t* sa = starts.data() + static_cast<size_t>(a) * d;
            const int64_t* sb = starts.data() + static_cast<size_t>(b) * d;
            const auto cmp =
                std::lexicographical_compare_three_way(sa, sa + d, sb, sb + d);
            if (cmp != 0) {
              return cmp < 0;
            }
            return sizes[a] > sizes[b];
          })};
}

// The seven greedy_by_* sort orders over one shared adjacency, optionally
// followed by the three time-derived orders of `surrogate`; place_portfolio
// races them all. Surrogate orders append, never displacing an input winner.
std::vector<std::vector<int32_t>> greedy_orders(
    const std::vector<Allocation>& allocations,
    const std::vector<Allocation>* surrogate, const CsrAdjacency& adj,
    const std::vector<int64_t>& sizes) {
  const size_t n = allocations.size();
  std::vector<int64_t> loads(n);
  const auto degree = [&](int32_t i) {
    return adj.offsets[i + 1] - adj.offsets[i];
  };
  for (size_t i = 0; i < n; ++i) {
    loads[i] = saturating_product(degree(static_cast<int32_t>(i)), sizes[i]);
  }
  std::vector<int32_t> base(n);
  std::iota(base.begin(), base.end(), 0);
  auto [by_duration, by_area, by_start] = time_orders(allocations, sizes, base);
  std::vector<std::vector<int32_t>> orders;
  orders.reserve(10);
  orders.push_back(base);  // greedy (input order)
  orders.push_back(
      sorted_by(base,  // greedy_by_size
                [&](int32_t a, int32_t b) { return sizes[a] > sizes[b]; }));
  orders.push_back(std::move(by_duration));
  orders.push_back(std::move(by_area));
  orders.push_back(
      sorted_by(base, [&](int32_t a, int32_t b) {  // greedy_by_conflict
        return std::pair(degree(a), sizes[a]) > std::pair(degree(b), sizes[b]);
      }));
  orders.push_back(
      sorted_by(base, [&](int32_t a, int32_t b) {  // greedy_by_conflict_size
        return std::pair(loads[a], sizes[a]) > std::pair(loads[b], sizes[b]);
      }));
  orders.push_back(std::move(by_start));
  if (surrogate != nullptr) {
    // Duplicates of input-clock orders never change the winner (ties favor
    // earlier orders); dropping them just skips redundant first-fit passes
    for (auto& order : time_orders(*surrogate, sizes, base)) {
      if (std::ranges::find(orders, order) == orders.end()) {
        orders.push_back(std::move(order));
      }
    }
  }
  return orders;
}

}  // namespace

PortfolioPlacement place_portfolio(const std::vector<Allocation>& allocations,
                                   const CsrAdjacency& adj,
                                   const std::vector<Allocation>* surrogate) {
  if (surrogate != nullptr && surrogate->size() != allocations.size()) {
    throw std::invalid_argument(
        "surrogate must be index-aligned with allocations");
  }
  const size_t n = allocations.size();
  std::vector<int64_t> sizes(n);
  std::ranges::transform(allocations, sizes.begin(), &Allocation::size);
  std::vector<int64_t> pins(n);
  std::ranges::transform(
      allocations, pins.begin(), [](const Allocation& alloc) {
        return alloc.offset().value_or(-1);  // -1 marks a free allocation
      });
  const auto orders = greedy_orders(allocations, surrogate, adj, sizes);

  // Placements are independent given the shared adjacency; threads only pay
  // off once the placements dwarf startup cost. One order per scheduled unit,
  // under the same worker ceiling as every other kernel.
  std::vector<std::vector<int64_t>> placements(orders.size());
  const unsigned workers =
      n < kMinParallel
          ? 1U
          : std::min<unsigned>(max_threads(),
                               static_cast<unsigned>(orders.size()));
  for_each_row_block(
      orders.size(), workers,
      [&](size_t v) {
        placements[v] = place_order(adj, sizes, pins, orders[v]);
      },
      1);

  // Strictly ordered reduction: ties break by the fixed order sequence, so
  // the winner never depends on how the placements were scheduled
  PortfolioPlacement best;
  best.peak = std::numeric_limits<int64_t>::max();
  for (auto& offsets : placements) {
    int64_t peak = 0;
    for (size_t i = 0; i < n; ++i) {
      peak = std::max(peak, offsets[i] + sizes[i]);
    }
    if (peak < best.peak) {
      best.peak = peak;
      best.offsets = std::move(offsets);
    }
  }
  return best;
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
    const std::vector<Allocation>& allocations,
    const ConflictIndices& indices) {
  // Lambda rather than the function pointer so the placement loop inlines
  // the offset scan instead of an indirect call per allocation
  return place_indexed(allocations, indices,
                       [](int64_t size, const auto& spans) {
                         return first_fit_offset(size, spans);
                       });
}

std::vector<Allocation> first_fit_place(
    const std::vector<Allocation>& allocations) {
  return first_fit_place_indexed(allocations,
                                 compute_conflict_indices(allocations));
}

FirstFitPlacer::FirstFitPlacer(std::vector<Allocation> allocations)
    : allocations_(std::move(allocations)),
      indices_(compute_conflict_indices(allocations_)) {
  check_total_size(allocations_);
}

void FirstFitPlacer::check_order(const std::vector<size_t>& order) const {
  std::vector<bool> seen(allocations_.size());
  for (size_t idx : order) {
    if (idx >= allocations_.size()) {
      throw std::invalid_argument(
          "order index " + std::to_string(idx) + " out of range for " +
          std::to_string(allocations_.size()) + " allocations");
    }
    if (seen[idx]) {
      throw std::invalid_argument("order index " + std::to_string(idx) +
                                  " appears more than once");
    }
    seen[idx] = true;
  }
}

std::vector<std::optional<int64_t>> FirstFitPlacer::place_offsets(
    const std::vector<size_t>& order) const {
  // Pre-set offsets are pins: obstacles from the first scan, never re-placed
  std::vector<std::optional<int64_t>> offsets(allocations_.size());
  for (size_t i = 0; i < allocations_.size(); ++i) {
    offsets[i] = allocations_[i].offset();
  }
  std::vector<std::pair<int64_t, int64_t>> spans;
  for (size_t idx : order) {
    const Allocation& alloc = allocations_[idx];
    if (alloc.offset().has_value()) {
      continue;
    }
    gather_spans(indices_[idx], offsets, allocations_, spans);
    offsets[idx] = first_fit_offset(alloc.size(), spans);
  }
  return offsets;
}

std::vector<Allocation> FirstFitPlacer::place(
    const std::vector<size_t>& order) const {
  check_order(order);
  const auto offsets = place_offsets(order);
  std::vector<Allocation> placed;
  placed.reserve(order.size());
  for (size_t idx : order) {
    placed.push_back(allocations_[idx].with_offset(*offsets[idx]));
  }
  return placed;
}

int64_t FirstFitPlacer::peak(const std::vector<size_t>& order) const {
  check_order(order);
  const auto offsets = place_offsets(order);
  int64_t peak = 0;
  for (size_t idx : order) {
    peak = std::max(peak, *offsets[idx] + allocations_[idx].size());
  }
  return peak;
}

}  // namespace omnimalloc
