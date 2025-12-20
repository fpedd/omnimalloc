//
// SPDX-License-Identifier: Apache-2.0
//

#include "greedy.hpp"

#include <algorithm>
#include <tuple>

namespace omnimalloc {

std::unordered_map<IdType, std::unordered_set<IdType, IdTypeHash>, IdTypeHash>
GreedyAllocator::compute_temporal_overlaps(
    const std::vector<Allocation>& allocations) {
  std::vector<std::tuple<int64_t, bool, size_t>> events;
  events.reserve(allocations.size() * 2);
  for (size_t i = 0; i < allocations.size(); ++i) {
    events.emplace_back(allocations[i].start(), true, i);
    events.emplace_back(allocations[i].end(), false, i);
  }

  // Sort events by time
  std::sort(events.begin(), events.end());

  std::unordered_map<IdType, std::unordered_set<IdType, IdTypeHash>, IdTypeHash>
      overlaps;
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

int64_t GreedyAllocator::find_best_offset(
    const Allocation& current_alloc,
    const std::vector<Allocation>& placed_allocations,
    const std::unordered_map<IdType, std::unordered_set<IdType, IdTypeHash>,
                             IdTypeHash>& overlaps) const {
  // Collect overlapping allocations that have been placed
  std::vector<const Allocation*> overlapping;
  auto it = overlaps.find(current_alloc.id());
  if (it != overlaps.end()) {
    overlapping.reserve(it->second.size());
    for (const auto& placed : placed_allocations) {
      if (it->second.count(placed.id())) {
        overlapping.push_back(&placed);
      }
    }
  }

  // Sort by offset to enable first-fit algorithm
  std::sort(overlapping.begin(), overlapping.end(),
            [](const Allocation* a, const Allocation* b) {
              return a->offset().value() < b->offset().value();
            });

  // Find best offset using first-fit: scan for first gap that fits
  int64_t best_offset = 0;
  for (const auto* placed : overlapping) {
    int64_t gap = placed->offset().value() - best_offset;
    if (gap >= current_alloc.size()) {
      break;  // Found a fitting gap
    }
    best_offset =
        std::max(best_offset, placed->offset().value() + placed->size());
  }

  return best_offset;
}

std::vector<Allocation> GreedyAllocator::allocate(
    const std::vector<Allocation>& allocations) const {
  const auto overlaps = compute_temporal_overlaps(allocations);

  std::vector<Allocation> placed_allocations;
  placed_allocations.reserve(allocations.size());
  for (const auto& alloc : allocations) {
    int64_t best_offset = find_best_offset(alloc, placed_allocations, overlaps);
    placed_allocations.push_back(alloc.with_offset(best_offset));
  }

  return placed_allocations;
}

}  // namespace omnimalloc

namespace std {

size_t hash<omnimalloc::GreedyAllocator>::operator()(
    const omnimalloc::GreedyAllocator&) const noexcept {
  // Stateless class - all instances are equal, use constant hash
  return 0x9e3779b9;  // arbitrary constant
}

}  // namespace std
