//
// SPDX-License-Identifier: Apache-2.0
//

#include "placement.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "clock.hpp"
#include "common/parallel.hpp"
#include "linearize.hpp"

// Reads pressure off an existing placement instead of solving for it: an
// allocation's conflict neighbors occupy disjoint ranges below their top, so
// that top is an upper bound. Interval orders paint; partial ones sweep.

namespace omnimalloc {

namespace {

// Half-open slot range [first, second) an allocation is live in.
using SlotWindow = std::pair<size_t, size_t>;

// Highest occupied address live in each compressed time slot. Painting
// tallest-first fills each slot exactly once (first writer wins, a union-find
// skips the rest), so the pass is one sort plus an amortized linear walk.
std::vector<int64_t> slot_tops(const std::vector<SlotWindow>& windows,
                               const std::vector<int64_t>& heights,
                               size_t slots) {
  const size_t n = windows.size();
  std::vector<size_t> tallest_first(n);
  std::iota(tallest_first.begin(), tallest_first.end(), 0);
  std::ranges::sort(tallest_first, [&](size_t a, size_t b) {
    return heights[a] > heights[b];
  });
  // next[j]: first still-unpainted slot at or after j, self-looping while
  // unpainted; the extra entry at `slots` is the walk's stop sentinel
  std::vector<size_t> next(slots + 1);
  std::iota(next.begin(), next.end(), 0);
  const auto unpainted_from = [&next](size_t j) {  // with path halving
    while (next[j] != j) {
      next[j] = next[next[j]];
      j = next[j];
    }
    return j;
  };
  std::vector<int64_t> tops(slots, 0);
  for (const size_t i : tallest_first) {
    const auto [lo, hi] = windows[i];
    for (size_t j = unpainted_from(lo); j < hi; j = unpainted_from(j + 1)) {
      tops[j] = heights[i];
      next[j] = j + 1;
    }
  }
  return tops;
}

std::vector<int64_t> scalar_peaks(
    const std::vector<std::pair<int64_t, int64_t>>& times,
    const std::vector<int64_t>& heights) {
  const std::vector<int64_t> bounds = slot_bounds(times);
  const size_t n = times.size();
  // Compressed once, then reused by both the paint pass and the queries
  std::vector<SlotWindow> windows(n);
  for (size_t i = 0; i < n; ++i) {
    windows[i] = {slot_index(bounds, times[i].first),
                  slot_index(bounds, times[i].second)};
  }
  const MaxSegtree tops(slot_tops(windows, heights, bounds.size()));
  std::vector<int64_t> peaks(n);
  for (size_t i = 0; i < n; ++i) {
    peaks[i] = tops.max(windows[i].first, windows[i].second);
  }
  return peaks;
}

std::vector<int64_t> vector_peaks(const ConflictSweep& sweep,
                                  const std::vector<int64_t>& heights) {
  const size_t n = heights.size();
  std::vector<std::atomic<int64_t>> top(n);
  for (size_t i = 0; i < n; ++i) {
    top[i].store(heights[i], std::memory_order_relaxed);
  }
  sweep.for_each_pair(parallel_threads(n), [&](size_t i, size_t j) {
    atomic_fetch_max(top[i], heights[j]);
    atomic_fetch_max(top[j], heights[i]);
  });
  std::vector<int64_t> peaks(n);
  for (size_t i = 0; i < n; ++i) {
    peaks[i] = top[i].load(std::memory_order_relaxed);
  }
  return peaks;
}

}  // namespace

std::vector<int64_t> placement_pressure_per_allocation(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget) {
  const size_t n = allocations.size();
  if (n == 0) {
    return {};
  }
  std::vector<int64_t> heights(n);
  for (size_t i = 0; i < n; ++i) {
    if (!allocations[i].offset().has_value()) {
      throw std::invalid_argument(
          "Per-allocation placement pressure requires placed allocations");
    }
    heights[i] = *allocations[i].offset() + allocations[i].size();
  }
  // Linearization preserves the conflict relation exactly, so neighborhood
  // tops transfer verbatim to the surrogate timeline.
  if (const auto times = linearize_times(allocations, work_budget)) {
    return scalar_peaks(*times, heights);
  }
  ClockSpans spans = gather_clock_spans(allocations);
  const std::vector<int64_t> backing = reduce_columns(spans);
  const ConflictSweep sweep(spans.starts, spans.ends, spans.dim);
  if (work_budget && sweep.sweep_work() > *work_budget) {
    throw std::runtime_error(
        "Conflict sweep work exceeds work_budget; pass None to always "
        "compute the placement pressure");
  }
  return vector_peaks(sweep, heights);
}

}  // namespace omnimalloc
