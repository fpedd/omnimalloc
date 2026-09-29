//
// SPDX-License-Identifier: Apache-2.0
//

#include "first_fit.hpp"

#include <algorithm>
#include <array>
#include <compare>
#include <cstdint>
#include <functional>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>
#include <utility>

#include "analysis/conflicts.hpp"
#include "common/hash.hpp"
#include "common/parallel.hpp"

namespace omnimalloc {

namespace {

constexpr std::array kGreedyOrders{
    GreedyOrder::INPUT, GreedyOrder::SIZE,     GreedyOrder::DURATION,
    GreedyOrder::AREA,  GreedyOrder::CONFLICT, GreedyOrder::CONFLICT_SIZE,
    GreedyOrder::START};

// The orders a surrogate linearization's times contribute
constexpr std::array kTimeOrders{GreedyOrder::DURATION, GreedyOrder::AREA,
                                 GreedyOrder::START};

// LSD radix sort by offset (the end rides along as payload; equal-offset order
// is irrelevant to the gap scans). Replaces the comparison sort that dominated
// first-fit at scale; pass count scales with the actual offset magnitude.
void sort_by_offset(std::vector<Interval>& intervals,
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

// Saturating product for the conflict x size key: a raw int64 product
// overflows (UB) on legal inputs (Allocation::area() saturates the same way)
int64_t saturating_product(int64_t a, int64_t b) noexcept {
  if (a > 0 && b > std::numeric_limits<int64_t>::max() / a) {
    return std::numeric_limits<int64_t>::max();
  }
  return a * b;
}

// Start components in canonical lane order, row-major (n x d): ordering lanes
// by their own contents stops arbitrary lane labelling from deciding the order.
std::vector<int64_t> canonical_starts(const std::vector<Allocation>& times,
                                      size_t d) {
  const size_t n = times.size();
  std::vector<size_t> lanes(d);
  std::iota(lanes.begin(), lanes.end(), 0);
  if (d > 1) {
    std::vector<uint64_t> fingerprint(d, 0);
    for (const Allocation& time : times) {
      const auto start = time.start_vec();
      const auto end = time.end_vec();
      for (size_t c = 0; c < d; ++c) {
        fingerprint[c] = hash_combine(
            hash_combine(fingerprint[c], static_cast<uint64_t>(start[c])),
            static_cast<uint64_t>(end[c]));
      }
    }
    const auto content_less = [&](size_t a, size_t b) {
      for (const Allocation& time : times) {
        const auto start = time.start_vec();
        const auto end = time.end_vec();
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

int64_t peak_of(const std::vector<int64_t>& offsets,
                const std::vector<int64_t>& sizes) {
  int64_t peak = 0;
  for (size_t i = 0; i < offsets.size(); ++i) {
    peak = std::max(peak, offsets[i] + sizes[i]);
  }
  return peak;
}

}  // namespace

int64_t first_fit_offset(int64_t size,
                         std::span<const Interval> spans) noexcept {
  int64_t best_offset = 0;
  for (const auto& [offset, end] : spans) {
    if (offset - best_offset >= size) {
      break;  // Found a fitting gap
    }
    best_offset = std::max(best_offset, end);
  }
  return best_offset;
}

std::vector<int64_t> pins_of(const std::vector<Allocation>& allocations) {
  std::vector<int64_t> pins(allocations.size());
  std::ranges::transform(allocations, pins.begin(), [](const Allocation& a) {
    return a.offset().value_or(-1);
  });
  return pins;
}

std::vector<int64_t> place_order(const CsrAdjacency& adj,
                                 const std::vector<int64_t>& sizes,
                                 const std::vector<int64_t>& pins,
                                 const std::vector<size_t>& order,
                                 ChooseOffset choose_offset) {
  std::vector<int64_t> offsets = pins;
  std::vector<Interval> spans;
  std::vector<Interval> scratch;
  for (const size_t i : order) {
    if (pins[i] >= 0) {
      continue;
    }
    spans.clear();
    for (const int32_t neighbor : adj.row(i)) {
      const auto j = static_cast<size_t>(neighbor);
      if (offsets[j] >= 0) {
        spans.emplace_back(offsets[j], offsets[j] + sizes[j]);
      }
    }
    sort_by_offset(spans, scratch);
    offsets[i] = choose_offset(sizes[i], spans);
  }
  return offsets;
}

std::vector<size_t> greedy_order(const std::vector<Allocation>& times,
                                 const CsrAdjacency& adj, GreedyOrder which) {
  const size_t n = times.size();
  std::vector<size_t> order(n);
  std::iota(order.begin(), order.end(), size_t{0});
  const auto descending = [&](auto key) {
    std::vector<decltype(key(size_t{0}))> keys(n);
    for (size_t i = 0; i < n; ++i) {
      keys[i] = key(i);
    }
    std::ranges::stable_sort(order, std::ranges::greater{},
                             [&](size_t i) { return keys[i]; });
  };
  const auto size = [&](size_t i) { return times[i].size(); };
  const auto degree = [&](size_t i) {
    return static_cast<int64_t>(adj.row(i).size());
  };
  switch (which) {
    case GreedyOrder::INPUT:
      break;
    case GreedyOrder::SIZE:
      descending(size);
      break;
    case GreedyOrder::DURATION:
      descending([&](size_t i) { return times[i].duration(); });
      break;
    case GreedyOrder::AREA:
      descending([&](size_t i) { return times[i].area(); });
      break;
    case GreedyOrder::CONFLICT:
      descending([&](size_t i) { return std::pair(degree(i), size(i)); });
      break;
    case GreedyOrder::CONFLICT_SIZE:
      descending([&](size_t i) {
        return std::pair(saturating_product(degree(i), size(i)), size(i));
      });
      break;
    case GreedyOrder::START: {
      const size_t d = n == 0 ? 1 : times[0].dim();
      const std::vector<int64_t> starts = canonical_starts(times, d);
      const auto start = [&](size_t i) {
        return std::span<const int64_t>{starts.data() + i * d, d};
      };
      std::ranges::stable_sort(order, [&](size_t a, size_t b) {
        const auto cmp = std::lexicographical_compare_three_way(
            start(a).begin(), start(a).end(), start(b).begin(), start(b).end());
        return cmp != 0 ? cmp < 0 : size(a) > size(b);
      });
      break;
    }
  }
  return order;
}

std::vector<Allocation> greedy_place(const std::vector<Allocation>& allocations,
                                     GreedyOrder which,
                                     ChooseOffset choose_offset) {
  check_total_size(allocations);
  const CsrAdjacency adj = build_conflict_adjacency(allocations);
  return apply_offsets(
      allocations,
      place_order(adj, sizes_of(allocations), pins_of(allocations),
                  greedy_order(allocations, adj, which), choose_offset));
}

std::vector<int64_t> place_portfolio(const std::vector<Allocation>& allocations,
                                     const CsrAdjacency& adj,
                                     const std::vector<Allocation>* surrogate) {
  std::vector<std::vector<size_t>> orders;
  for (const GreedyOrder which : kGreedyOrders) {
    orders.push_back(greedy_order(allocations, adj, which));
  }
  if (surrogate != nullptr) {
    // Duplicates of input-clock orders never change the winner (ties favor
    // earlier orders); dropping them just skips redundant placements
    for (const GreedyOrder which : kTimeOrders) {
      auto order = greedy_order(*surrogate, adj, which);
      if (std::ranges::find(orders, order) == orders.end()) {
        orders.push_back(std::move(order));
      }
    }
  }

  // Placements are independent given the shared adjacency; threads only pay
  // off once the placements dwarf startup cost. One order per scheduled unit,
  // under the same worker ceiling as every other kernel.
  const std::vector<int64_t> sizes = sizes_of(allocations);
  const std::vector<int64_t> pins = pins_of(allocations);
  std::vector<std::vector<int64_t>> placements(orders.size());
  const unsigned workers =
      allocations.size() < kMinParallel
          ? 1U
          : std::min<unsigned>(max_threads(),
                               static_cast<unsigned>(orders.size()));
  for_each_row_block(
      orders.size(), workers,
      [&](size_t v) {
        placements[v] =
            place_order(adj, sizes, pins, orders[v], first_fit_offset);
      },
      1);

  // min_element keeps the first minimum, so ties break by the fixed order
  // sequence and never by how the placements were scheduled
  std::vector<int64_t> peaks;
  for (const auto& offsets : placements) {
    peaks.push_back(peak_of(offsets, sizes));
  }
  return std::move(placements[static_cast<size_t>(
      std::ranges::min_element(peaks) - peaks.begin())]);
}

FirstFitPlacer::FirstFitPlacer(std::vector<Allocation> allocations)
    : allocations_(std::move(allocations)),
      adj_(build_conflict_adjacency(allocations_)),
      sizes_(sizes_of(allocations_)),
      pins_(pins_of(allocations_)) {
  check_total_size(allocations_);
}

std::vector<int64_t> FirstFitPlacer::checked_offsets(
    const std::vector<size_t>& order) const {
  std::vector<bool> seen(allocations_.size());
  for (const size_t i : order) {
    if (i >= allocations_.size()) {
      throw std::invalid_argument(
          "order index " + std::to_string(i) + " out of range for " +
          std::to_string(allocations_.size()) + " allocations");
    }
    if (seen[i]) {
      throw std::invalid_argument("order index " + std::to_string(i) +
                                  " appears more than once");
    }
    seen[i] = true;
  }
  return place_order(adj_, sizes_, pins_, order, first_fit_offset);
}

std::vector<Allocation> FirstFitPlacer::place(
    const std::vector<size_t>& order) const {
  const std::vector<int64_t> offsets = checked_offsets(order);
  std::vector<Allocation> placed;
  placed.reserve(order.size());
  for (const size_t i : order) {
    placed.push_back(allocations_[i].with_offset(offsets[i]));
  }
  return placed;
}

int64_t FirstFitPlacer::peak(const std::vector<size_t>& order) const {
  const std::vector<int64_t> offsets = checked_offsets(order);
  int64_t peak = 0;
  for (const size_t i : order) {
    peak = std::max(peak, offsets[i] + sizes_[i]);
  }
  return peak;
}

}  // namespace omnimalloc
