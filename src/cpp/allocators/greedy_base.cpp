//
// SPDX-License-Identifier: Apache-2.0
//

#include "greedy_base.hpp"

#include <algorithm>
#include <numeric>
#include <tuple>
#include <utility>

namespace omnimalloc {

TemporalOverlaps compute_temporal_overlaps(
    const std::vector<Allocation>& allocations) {
  std::vector<std::tuple<int64_t, bool, size_t>> events;
  events.reserve(allocations.size() * 2);
  for (size_t i = 0; i < allocations.size(); ++i) {
    events.emplace_back(allocations[i].start(), true, i);
    events.emplace_back(allocations[i].end(), false, i);
  }

  // Sort events by time
  std::sort(events.begin(), events.end());

  TemporalOverlaps overlaps;
  std::unordered_set<size_t> active;
  for (const auto& [time, is_start, idx] : events) {
    if (is_start) {
      // Current allocation overlaps with all currently active allocations
      for (size_t active_idx : active) {
        overlaps[allocations[idx].id()].insert(allocations[active_idx].id());
        overlaps[allocations[active_idx].id()].insert(allocations[idx].id());
      }
      active.insert(idx);
    } else {
      active.erase(idx);
    }
  }

  return overlaps;
}

int64_t peak_of(const std::vector<Allocation>& placed) {
  int64_t peak = 0;
  for (const auto& alloc : placed) {
    if (const auto height = alloc.height()) {
      peak = std::max(peak, *height);
    }
  }
  return peak;
}

std::vector<const Allocation*> placed_overlapping(
    const Allocation& alloc, const std::vector<Allocation>& placed,
    const TemporalOverlaps& overlaps) {
  // Collect overlapping allocations that have been placed
  std::vector<const Allocation*> overlapping;
  auto it = overlaps.find(alloc.id());
  if (it != overlaps.end()) {
    overlapping.reserve(it->second.size());
    for (const auto& candidate : placed) {
      if (it->second.count(candidate.id())) {
        overlapping.push_back(&candidate);
      }
    }
  }

  // Sort by offset so callers can scan the free gaps left-to-right
  std::sort(overlapping.begin(), overlapping.end(),
            [](const Allocation* a, const Allocation* b) {
              return a->offset().value() < b->offset().value();
            });

  return overlapping;
}

std::vector<size_t> initial_order(const std::vector<Allocation>& allocations) {
  std::vector<size_t> order(allocations.size());
  std::iota(order.begin(), order.end(), size_t{0});
  std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
    return allocations[a].size() > allocations[b].size();
  });
  return order;
}

std::vector<size_t> earlier_neighbors(
    const std::vector<size_t>& order, size_t target_pos,
    const std::vector<Allocation>& allocations,
    const TemporalOverlaps& overlaps) {
  std::vector<size_t> neighbors;
  auto it = overlaps.find(allocations[order[target_pos]].id());
  if (it != overlaps.end()) {
    for (size_t pos = 0; pos < target_pos; ++pos) {
      if (it->second.count(allocations[order[pos]].id())) {
        neighbors.push_back(pos);
      }
    }
  }
  if (neighbors.empty()) {
    neighbors.resize(target_pos);
    std::iota(neighbors.begin(), neighbors.end(), size_t{0});
  }
  return neighbors;
}

namespace {

int64_t find_best_offset(const Allocation& current_alloc,
                         const std::vector<Allocation>& placed_allocations,
                         const TemporalOverlaps& overlaps) {
  // Find best offset using first-fit: scan for first gap that fits
  int64_t best_offset = 0;
  for (const auto* placed :
       placed_overlapping(current_alloc, placed_allocations, overlaps)) {
    int64_t gap = placed->offset().value() - best_offset;
    if (gap >= current_alloc.size()) {
      break;  // Found a fitting gap
    }
    best_offset =
        std::max(best_offset, placed->offset().value() + placed->size());
  }

  return best_offset;
}

}  // namespace

std::vector<Allocation> first_fit_place(
    const std::vector<Allocation>& allocations,
    const TemporalOverlaps& overlaps) {
  std::vector<Allocation> placed_allocations;
  placed_allocations.reserve(allocations.size());
  for (const auto& alloc : allocations) {
    int64_t best_offset = find_best_offset(alloc, placed_allocations, overlaps);
    placed_allocations.push_back(alloc.with_offset(best_offset));
  }

  return placed_allocations;
}

FirstFitPlacer::FirstFitPlacer(std::vector<Allocation> allocations)
    : allocations_(std::move(allocations)),
      overlaps_(compute_temporal_overlaps(allocations_)) {}

std::vector<Allocation> FirstFitPlacer::place(
    const std::vector<size_t>& order) const {
  std::vector<Allocation> ordered;
  ordered.reserve(order.size());
  for (size_t idx : order) {
    ordered.push_back(allocations_.at(idx));
  }
  return first_fit_place(ordered, overlaps_);
}

int64_t FirstFitPlacer::evaluate(const std::vector<size_t>& order) const {
  return peak_of(place(order));
}

}  // namespace omnimalloc
