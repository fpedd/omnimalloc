//
// SPDX-License-Identifier: Apache-2.0
//

#include "conflicts.hpp"

#include <algorithm>
#include <atomic>
#include <tuple>
#include <utility>

#include "common/parallel.hpp"

namespace omnimalloc {

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

// The pruned pairwise sweep itself lives in analysis/clock.hpp
// (ConflictSweep), shared with the exact per-allocation pressure kernels.
ConflictSweep build_conflict_sweep(const std::vector<Allocation>& allocations) {
  const ClockSpans spans = gather_clock_spans(allocations);
  return {spans.starts, spans.ends, spans.dim};
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

CsrAdjacency build_conflict_adjacency(
    const std::vector<Allocation>& allocations) {
  const ConflictSweep sweep = build_conflict_sweep(allocations);
  return sweep.adjacency(parallel_threads(sweep.count()));
}

TemporalOverlaps overlaps_from_indices(
    const std::vector<Allocation>& allocations, const OverlapIndices& indices) {
  TemporalOverlaps overlaps;
  for (size_t i = 0; i < allocations.size(); ++i) {
    for (size_t j : indices[i]) {
      overlaps[allocations[i].id()].insert(allocations[j].id());
    }
  }
  return overlaps;
}

OverlapIndices compute_overlap_indices(
    const std::vector<Allocation>& allocations) {
  const bool all_scalar =
      std::ranges::all_of(allocations, &Allocation::is_scalar_time);
  return all_scalar ? scalar_overlap_indices(allocations)
                    : vector_overlap_indices(allocations);
}

std::optional<TemporalOverlaps> compute_temporal_overlaps(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget) {
  // On scalar input the sweep work equals the exact overlap-pair count, the
  // scalar sweep line's dominant cost, so one measure bounds both paths.
  if (work_budget &&
      build_conflict_sweep(allocations).sweep_work() > *work_budget) {
    return std::nullopt;
  }
  return overlaps_from_indices(allocations,
                               compute_overlap_indices(allocations));
}

std::optional<std::vector<int64_t>> compute_conflict_degrees(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget) {
  const ConflictSweep sweep = build_conflict_sweep(allocations);
  if (work_budget && sweep.sweep_work() > *work_budget) {
    return std::nullopt;
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
