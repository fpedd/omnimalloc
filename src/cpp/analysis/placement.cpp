//
// SPDX-License-Identifier: Apache-2.0
//

#include "placement.hpp"

#include <algorithm>
#include <atomic>
#include <functional>
#include <set>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

#include "clock.hpp"
#include "common/parallel.hpp"
#include "linearize.hpp"

// Reads pressure off an existing placement instead of solving for it:
// everything live together with an allocation conflicts with it and
// occupies disjoint address ranges below the neighborhood's top, so the
// top certifies an upper bound without any search. Interval orders take
// one single-timeline sweep with range-max queries; genuinely partial
// orders take the pruned pairwise conflict sweep.

namespace omnimalloc {

namespace {

// Highest occupied address live in each compressed time slot, via an event
// sweep over an ordered height multiset (ends release before starts claim).
std::vector<int64_t> slot_tops(
    const std::vector<std::pair<int64_t, int64_t>>& times,
    const std::vector<int64_t>& heights, const std::vector<int64_t>& bounds) {
  const size_t n = times.size();
  std::vector<std::pair<size_t, int64_t>> starts_at(n);
  std::vector<std::pair<size_t, int64_t>> ends_at(n);
  for (size_t i = 0; i < n; ++i) {
    starts_at[i] = {slot_index(bounds, times[i].first), heights[i]};
    ends_at[i] = {slot_index(bounds, times[i].second), heights[i]};
  }
  std::ranges::sort(starts_at);
  std::ranges::sort(ends_at);
  std::multiset<int64_t> active;
  std::vector<int64_t> tops(bounds.size(), 0);
  size_t started = 0;
  size_t ended = 0;
  for (size_t j = 0; j < bounds.size(); ++j) {
    while (ended < n && ends_at[ended].first == j) {
      active.erase(active.find(ends_at[ended].second));
      ++ended;
    }
    while (started < n && starts_at[started].first == j) {
      active.insert(starts_at[started].second);
      ++started;
    }
    tops[j] = active.empty() ? 0 : *active.rbegin();
  }
  return tops;
}

std::vector<int64_t> scalar_peaks(
    const std::vector<std::pair<int64_t, int64_t>>& times,
    const std::vector<int64_t>& heights) {
  const std::vector<int64_t> bounds = slot_bounds(times);
  const MaxSegtree tops(slot_tops(times, heights, bounds));
  std::vector<int64_t> peaks(times.size());
  for (size_t i = 0; i < times.size(); ++i) {
    peaks[i] = tops.max(slot_index(bounds, times[i].first),
                        slot_index(bounds, times[i].second));
  }
  return peaks;
}

std::vector<int64_t> vector_peaks(const ClockSpans& spans,
                                  const std::vector<int64_t>& heights) {
  const size_t n = spans.starts.size();
  const ConflictSweep sweep(spans.starts, spans.ends, spans.dim);
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
    const std::vector<Allocation>& allocations) {
  const size_t n = allocations.size();
  if (n == 0) {
    return {};
  }
  const ClockSpans spans = gather_clock_spans(allocations);
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
  if (const auto times = linearize_times(allocations, std::nullopt)) {
    return scalar_peaks(*times, heights);
  }
  return vector_peaks(spans, heights);
}

}  // namespace omnimalloc
