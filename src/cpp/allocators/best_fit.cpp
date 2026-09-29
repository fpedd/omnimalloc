//
// SPDX-License-Identifier: Apache-2.0
//

#include "best_fit.hpp"

#include <algorithm>
#include <span>

#include "first_fit.hpp"

namespace omnimalloc {

namespace {

// The smallest gap between the spans that fits `size`, ties to the lowest
// offset, or past the last span when none does
int64_t best_fit_offset(int64_t size,
                        std::span<const Interval> spans) noexcept {
  int64_t cursor = 0;
  int64_t best_offset = 0;
  int64_t best_gap = -1;  // negative sentinel: no finite fitting gap yet
  for (const auto& [offset, end] : spans) {
    const int64_t gap = offset - cursor;
    if (gap >= size && (best_gap < 0 || gap < best_gap)) {
      best_gap = gap;
      best_offset = cursor;
    }
    cursor = std::max(cursor, end);
  }
  return best_gap < 0 ? cursor : best_offset;
}

}  // namespace

std::vector<Allocation> best_fit_place(
    const std::vector<Allocation>& allocations) {
  return greedy_place(allocations, GreedyOrder::INPUT, best_fit_offset);
}

}  // namespace omnimalloc
