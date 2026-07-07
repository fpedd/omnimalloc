//
// SPDX-License-Identifier: Apache-2.0
//

#include "best_fit.hpp"

#include <algorithm>

namespace omnimalloc {

namespace {

int64_t find_best_fit_offset(const Allocation& current_alloc,
                             const std::vector<Allocation>& placed_allocations,
                             const TemporalOverlaps& overlaps) {
  // Scan every gap between placed overlaps and keep the smallest that fits
  int64_t cursor = 0;
  int64_t best_offset = 0;
  int64_t best_gap = -1;  // negative sentinel: no finite fitting gap yet
  for (const auto* placed :
       placed_overlapping(current_alloc, placed_allocations, overlaps)) {
    int64_t gap = placed->offset().value() - cursor;
    if (gap >= current_alloc.size() && (best_gap < 0 || gap < best_gap)) {
      best_gap = gap;
      best_offset = cursor;
    }
    cursor = std::max(cursor, placed->offset().value() + placed->size());
  }

  // No finite gap fit: place after the last overlapping allocation
  return best_gap < 0 ? cursor : best_offset;
}

}  // namespace

std::vector<Allocation> BestFitAllocator::allocate(
    const std::vector<Allocation>& allocations) const {
  const TemporalOverlaps overlaps = compute_temporal_overlaps(allocations);
  std::vector<Allocation> placed_allocations;
  placed_allocations.reserve(allocations.size());
  for (const auto& alloc : allocations) {
    int64_t best_offset =
        find_best_fit_offset(alloc, placed_allocations, overlaps);
    placed_allocations.push_back(alloc.with_offset(best_offset));
  }

  return placed_allocations;
}

}  // namespace omnimalloc

namespace std {

size_t hash<omnimalloc::BestFitAllocator>::operator()(
    const omnimalloc::BestFitAllocator&) const noexcept {
  // Stateless class - all instances are equal, use constant hash
  return 0x517cc1b7;  // arbitrary constant
}

}  // namespace std
