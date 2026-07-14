//
// SPDX-License-Identifier: Apache-2.0
//

#include "best_fit.hpp"

#include <algorithm>

namespace omnimalloc {

namespace {

int64_t find_best_fit_offset(
    int64_t size, const std::vector<std::pair<int64_t, int64_t>>& spans) {
  // Scan every gap between the sorted placed spans, keep the smallest that fits
  int64_t cursor = 0;
  int64_t best_offset = 0;
  int64_t best_gap = -1;  // negative sentinel: no finite fitting gap yet
  for (const auto& [offset, end] : spans) {
    int64_t gap = offset - cursor;
    if (gap >= size && (best_gap < 0 || gap < best_gap)) {
      best_gap = gap;
      best_offset = cursor;
    }
    cursor = std::max(cursor, end);
  }

  // No finite gap fit: place after the last overlapping allocation
  return best_gap < 0 ? cursor : best_offset;
}

}  // namespace

std::vector<Allocation> BestFitAllocator::allocate(
    const std::vector<Allocation>& allocations) const {
  check_total_size(allocations);
  const OverlapIndices indices = compute_overlap_indices(allocations);
  std::vector<std::optional<int64_t>> offsets(allocations.size());
  std::vector<std::pair<int64_t, int64_t>> spans;
  std::vector<Allocation> placed_allocations;
  placed_allocations.reserve(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    spans.clear();
    for (size_t j : indices[i]) {
      if (offsets[j].has_value()) {
        spans.emplace_back(*offsets[j], *offsets[j] + allocations[j].size());
      }
    }
    std::sort(spans.begin(), spans.end());
    offsets[i] = find_best_fit_offset(allocations[i].size(), spans);
    placed_allocations.push_back(allocations[i].with_offset(*offsets[i]));
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
