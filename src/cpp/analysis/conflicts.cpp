//
// SPDX-License-Identifier: Apache-2.0
//

#include "conflicts.hpp"

#include <algorithm>
#include <atomic>
#include <stdexcept>
#include <tuple>
#include <utility>

#include "common/parallel.hpp"

namespace omnimalloc {

namespace {

// Sweep line over the single timeline; valid only for all-scalar input.
ConflictIndices scalar_conflict_indices(
    const std::vector<Allocation>& allocations) {
  std::vector<std::tuple<int64_t, bool, size_t>> events;
  events.reserve(allocations.size() * 2);
  for (size_t i = 0; i < allocations.size(); ++i) {
    events.emplace_back(allocations[i].start(), true, i);
    events.emplace_back(allocations[i].end(), false, i);
  }

  // Sort events by time; ends sort before starts at equal times, matching the
  // half-open interval semantics of Allocation::conflicts_with
  std::sort(events.begin(), events.end());

  ConflictIndices indices(allocations.size());
  std::vector<size_t> active;
  for (const auto& [time, is_start, idx] : events) {
    if (is_start) {
      // Current allocation conflicts with all currently active allocations
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

// Degrees on the single timeline without enumerating pairs: allocation i
// conflicts with everything that starts before its end minus everything
// already ended by its start (half-open lifetimes); the -1 removes i itself.
std::vector<int64_t> scalar_conflict_degrees(
    const std::vector<Allocation>& allocations) {
  const size_t n = allocations.size();
  std::vector<int64_t> starts(n);
  std::vector<int64_t> ends(n);
  for (size_t i = 0; i < n; ++i) {
    starts[i] = allocations[i].start();
    ends[i] = allocations[i].end();
  }
  std::ranges::sort(starts);
  std::ranges::sort(ends);
  std::vector<int64_t> degrees(n);
  for (size_t i = 0; i < n; ++i) {
    const auto started =
        std::ranges::lower_bound(starts, allocations[i].end()) - starts.begin();
    const auto ended =
        std::ranges::upper_bound(ends, allocations[i].start()) - ends.begin();
    degrees[i] = started - ended - 1;
  }
  return degrees;
}

// The pruned pairwise sweep itself lives in analysis/clock.hpp
// (ConflictSweep), shared with the exact per-allocation pressure kernels.
ConflictSweep build_conflict_sweep(const std::vector<Allocation>& allocations) {
  const ClockSpans spans = gather_clock_spans(allocations);
  return {spans.starts, spans.ends, spans.dim};
}

// Pairwise happens-before adjacency; the only option for vector clocks
ConflictIndices vector_conflict_indices(
    const std::vector<Allocation>& allocations) {
  const CsrAdjacency adj = build_conflict_adjacency(allocations);
  ConflictIndices indices(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    indices[i].assign(adj.neighbors.begin() + adj.offsets[i],
                      adj.neighbors.begin() + adj.offsets[i + 1]);
  }
  return indices;
}

}  // namespace

CsrAdjacency build_conflict_adjacency(
    const std::vector<Allocation>& allocations) {
  const ConflictSweep sweep = build_conflict_sweep(allocations);
  return sweep.adjacency(parallel_threads(sweep.count()));
}

ConflictMap conflict_map_from_indices(
    const std::vector<Allocation>& allocations,
    const ConflictIndices& indices) {
  ConflictMap map;
  map.reserve(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    auto& neighbors = map[allocations[i].id()];
    for (size_t j : indices[i]) {
      neighbors.insert(allocations[j].id());
    }
  }
  return map;
}

ConflictIndices compute_conflict_indices(
    const std::vector<Allocation>& allocations) {
  const bool all_scalar =
      std::ranges::all_of(allocations, &Allocation::is_scalar_time);
  return all_scalar ? scalar_conflict_indices(allocations)
                    : vector_conflict_indices(allocations);
}

ConflictMap conflicts(const std::vector<Allocation>& allocations,
                      std::optional<uint64_t> work_budget) {
  // On scalar input the sweep work equals the exact conflict-pair count, the
  // scalar sweep line's dominant cost, so one measure bounds both paths.
  if (work_budget &&
      build_conflict_sweep(allocations).sweep_work() > *work_budget) {
    throw std::runtime_error(
        "Conflict sweep work exceeds work_budget; pass None to always "
        "compute the relation");
  }
  return conflict_map_from_indices(allocations,
                                   compute_conflict_indices(allocations));
}

std::vector<int64_t> conflict_degrees(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget) {
  // Scalar timelines count degrees in O(N log N) via two binary searches per
  // allocation; the budget guards only the pair-enumerating vector path.
  if (std::ranges::all_of(allocations, &Allocation::is_scalar_time)) {
    return scalar_conflict_degrees(allocations);
  }
  const ConflictSweep sweep = build_conflict_sweep(allocations);
  if (work_budget && sweep.sweep_work() > *work_budget) {
    throw std::runtime_error(
        "Conflict sweep work exceeds work_budget; pass None to always count");
  }
  std::vector<std::atomic<int64_t>> degree(sweep.count());
  sweep.for_each_pair(parallel_threads(sweep.count()), [&](size_t i, size_t j) {
    degree[i].fetch_add(1, std::memory_order_relaxed);
    degree[j].fetch_add(1, std::memory_order_relaxed);
  });
  std::vector<int64_t> degrees(sweep.count());
  for (size_t i = 0; i < sweep.count(); ++i) {
    degrees[i] = degree[i].load(std::memory_order_relaxed);
  }
  return degrees;
}

}  // namespace omnimalloc
