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
// top (and the clique total) certify upper bounds without any search.
// Interval orders take one single-timeline sweep with range-max queries;
// genuinely partial orders take the pruned pairwise conflict sweep.

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

// Conflict clique totals on the single timeline: everything started before
// each end minus everything already ended by each start (half-open
// lifetimes), each allocation's own weight included.
std::vector<int64_t> scalar_clique_sums(
    const std::vector<std::pair<int64_t, int64_t>>& times,
    const std::vector<int64_t>& weights) {
  const size_t n = times.size();
  std::vector<std::pair<int64_t, int64_t>> by_start(n);
  std::vector<std::pair<int64_t, int64_t>> by_end(n);
  for (size_t i = 0; i < n; ++i) {
    by_start[i] = {times[i].first, weights[i]};
    by_end[i] = {times[i].second, weights[i]};
  }
  std::ranges::sort(by_start);
  std::ranges::sort(by_end);
  std::vector<int64_t> started(n + 1, 0);
  std::vector<int64_t> ended(n + 1, 0);
  for (size_t i = 0; i < n; ++i) {
    started[i + 1] = started[i] + by_start[i].second;
    ended[i + 1] = ended[i] + by_end[i].second;
  }
  const auto weight_below =
      [](const std::vector<std::pair<int64_t, int64_t>>& events,
         const std::vector<int64_t>& prefix, int64_t bound) {
        const auto it = std::ranges::lower_bound(
            events, bound, std::less{}, &std::pair<int64_t, int64_t>::first);
        return prefix[static_cast<size_t>(it - events.begin())];
      };
  std::vector<int64_t> sums(n);
  for (size_t i = 0; i < n; ++i) {
    // starts strictly before the end, minus ends at or before the start
    sums[i] = weight_below(by_start, started, times[i].second) -
              weight_below(by_end, ended, times[i].first + 1);
  }
  return sums;
}

std::vector<int64_t> scalar_peaks(
    const std::vector<std::pair<int64_t, int64_t>>& times,
    const std::vector<int64_t>& heights, const std::vector<int64_t>& weights,
    bool clique_cap) {
  const std::vector<int64_t> bounds = slot_bounds(times);
  const MaxSegtree tops(slot_tops(times, heights, bounds));
  std::vector<int64_t> peaks(times.size());
  for (size_t i = 0; i < times.size(); ++i) {
    peaks[i] = tops.max(slot_index(bounds, times[i].first),
                        slot_index(bounds, times[i].second));
  }
  if (clique_cap) {
    const std::vector<int64_t> sums = scalar_clique_sums(times, weights);
    for (size_t i = 0; i < peaks.size(); ++i) {
      peaks[i] = std::min(peaks[i], sums[i]);
    }
  }
  return peaks;
}

std::vector<int64_t> vector_peaks(const ClockSpans& spans,
                                  const std::vector<int64_t>& heights,
                                  const std::vector<int64_t>& weights,
                                  bool clique_cap) {
  const size_t n = spans.starts.size();
  const ConflictSweep sweep(spans.starts, spans.ends, spans.dim);
  std::vector<std::atomic<int64_t>> top(n);
  std::vector<std::atomic<int64_t>> sum(n);
  for (size_t i = 0; i < n; ++i) {
    top[i].store(heights[i], std::memory_order_relaxed);
    sum[i].store(weights[i], std::memory_order_relaxed);
  }
  sweep.for_each_pair(parallel_threads(n), [&](size_t i, size_t j) {
    atomic_fetch_max(top[i], heights[j]);
    atomic_fetch_max(top[j], heights[i]);
    sum[i].fetch_add(weights[j], std::memory_order_relaxed);
    sum[j].fetch_add(weights[i], std::memory_order_relaxed);
  });
  std::vector<int64_t> peaks(n);
  for (size_t i = 0; i < n; ++i) {
    const int64_t peak = top[i].load(std::memory_order_relaxed);
    peaks[i] = clique_cap
                   ? std::min(peak, sum[i].load(std::memory_order_relaxed))
                   : peak;
  }
  return peaks;
}

}  // namespace

std::vector<int64_t> per_allocation_placement_pressure(
    const std::vector<Allocation>& allocations, bool clique_cap) {
  const size_t n = allocations.size();
  if (n == 0) {
    return {};
  }
  const ClockSpans spans = gather_clock_spans(allocations);
  std::vector<int64_t> heights(n);
  std::vector<int64_t> weights(n);
  for (size_t i = 0; i < n; ++i) {
    if (!allocations[i].offset().has_value()) {
      throw std::invalid_argument(
          "per-allocation placement pressure requires placed allocations");
    }
    heights[i] = *allocations[i].offset() + allocations[i].size();
    weights[i] = allocations[i].size();
  }
  // Linearization preserves the conflict relation exactly, so neighborhood
  // tops and clique sums transfer verbatim to the surrogate timeline.
  if (const auto times = linearize_times(allocations, std::nullopt)) {
    return scalar_peaks(*times, heights, weights, clique_cap);
  }
  return vector_peaks(spans, heights, weights, clique_cap);
}

}  // namespace omnimalloc
