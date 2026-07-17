//
// SPDX-License-Identifier: Apache-2.0
//

#include "first_fit.hpp"

#include <algorithm>
#include <cstring>
#include <future>
#include <limits>
#include <numeric>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/parallel.hpp"

namespace omnimalloc {

void require_scalar_time(const std::vector<Allocation>& allocations,
                         const char* who) {
  if (!std::ranges::all_of(allocations, &Allocation::is_scalar_time)) {
    throw std::invalid_argument(
        std::string(who) +
        " requires scalar time lifetimes; linearize vector clocks first");
  }
}

namespace {

// Occupied (offset, end) span of a placed allocation, matching the span
// shape that `first_fit_offset` consumes
using Interval = std::pair<int64_t, int64_t>;

// LSD radix sort by offset (the end rides along as payload; equal-offset
// order is irrelevant to the gap scan). The comparison sort dominated
// first-fit at scale, radix is ~5x cheaper; pass count scales with the
// actual offset magnitude, so no assumption on the offset range.
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
    std::memcpy(intervals.data(), src, m * sizeof(Interval));
  }
}

// First-fit offsets for the allocations taken in `order`, gathering each
// allocation's placed CSR neighbors and reusing the shared gap scan
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

// The seven greedy_by_* sort orders over one shared adjacency, mirroring the
// greedy_by_* allocators; place_portfolio races them all
std::vector<std::vector<int32_t>> greedy_orders(
    const std::vector<Allocation>& allocations, const CsrAdjacency& adj,
    const std::vector<int64_t>& sizes) {
  const size_t n = allocations.size();
  std::vector<int64_t> durations(n);
  std::vector<int64_t> areas(n);
  std::vector<int64_t> loads(n);
  const auto degree = [&](int32_t i) {
    return adj.offsets[i + 1] - adj.offsets[i];
  };
  for (size_t i = 0; i < n; ++i) {
    durations[i] = allocations[i].duration();
    areas[i] = allocations[i].area();
    loads[i] = saturating_product(degree(static_cast<int32_t>(i)), sizes[i]);
  }
  std::vector<int32_t> base(n);
  std::iota(base.begin(), base.end(), 0);
  const auto sorted_by = [&](auto&& less) {
    std::vector<int32_t> result = base;
    std::stable_sort(result.begin(), result.end(), less);
    return result;
  };
  std::vector<std::vector<int32_t>> orders;
  orders.reserve(7);
  orders.push_back(base);      // greedy (input order)
  orders.push_back(sorted_by(  // greedy_by_size
      [&](int32_t a, int32_t b) { return sizes[a] > sizes[b]; }));
  orders.push_back(sorted_by(  // greedy_by_duration
      [&](int32_t a, int32_t b) { return durations[a] > durations[b]; }));
  orders.push_back(sorted_by(  // greedy_by_area
      [&](int32_t a, int32_t b) { return areas[a] > areas[b]; }));
  orders.push_back(sorted_by([&](int32_t a, int32_t b) {  // greedy_by_conflict
    return std::pair(degree(a), sizes[a]) > std::pair(degree(b), sizes[b]);
  }));
  orders.push_back(
      sorted_by([&](int32_t a, int32_t b) {  // greedy_by_conflict_size
        return std::pair(loads[a], sizes[a]) > std::pair(loads[b], sizes[b]);
      }));
  orders.push_back(sorted_by([&](int32_t a, int32_t b) {  // greedy_by_start
    const auto sa = allocations[a].start_vec();
    const auto sb = allocations[b].start_vec();
    const auto cmp = std::lexicographical_compare_three_way(
        sa.begin(), sa.end(), sb.begin(), sb.end());
    if (cmp != 0) {
      return cmp < 0;
    }
    return sizes[a] > sizes[b];
  }));
  return orders;
}

}  // namespace

PortfolioPlacement place_portfolio(const std::vector<Allocation>& allocations,
                                   const CsrAdjacency& adj) {
  const size_t n = allocations.size();
  std::vector<int64_t> sizes(n);
  for (size_t i = 0; i < n; ++i) {
    sizes[i] = allocations[i].size();
  }
  const auto orders = greedy_orders(allocations, adj, sizes);

  // Placements are independent given the shared adjacency; spawning threads
  // only pays off once the placements dwarf the thread startup cost
  std::vector<std::vector<int64_t>> placements(orders.size());
  if (n < kMinParallel) {
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

  // Futures are joined before this strictly-ordered reduction, so ties break
  // by the fixed order sequence and the winner is deterministic
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
      indices_(compute_overlap_indices(allocations_)),
      overlaps_(overlaps_from_indices(allocations_, indices_)) {
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
