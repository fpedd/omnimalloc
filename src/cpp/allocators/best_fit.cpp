//
// SPDX-License-Identifier: Apache-2.0
//

#include "best_fit.hpp"

#include <algorithm>

#include "first_fit.hpp"

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

  // No finite gap fit: place after the last conflicting allocation
  return best_gap < 0 ? cursor : best_offset;
}

}  // namespace

std::vector<Allocation> best_fit_place(
    const std::vector<Allocation>& allocations) {
  // Lambda rather than the function pointer so the placement loop inlines
  // the offset scan instead of an indirect call per allocation
  return place_indexed(allocations, compute_conflict_indices(allocations),
                       [](int64_t size, const auto& spans) {
                         return find_best_fit_offset(size, spans);
                       });
}

}  // namespace omnimalloc
