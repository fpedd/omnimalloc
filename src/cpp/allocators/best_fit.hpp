//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Best-fit placement: like first-fit, but among the gaps left by
// already-placed overlapping allocations it picks the smallest one that fits
// (ties broken by lowest offset) rather than the first one found. Falls back
// to placing after the last overlapping allocation when no finite gap fits.
class BestFitAllocator {
 public:
  BestFitAllocator() = default;

  // Allocate the given allocations using a best-fit greedy strategy.
  std::vector<Allocation> allocate(
      const std::vector<Allocation>& allocations) const;

  bool operator==(const BestFitAllocator&) const noexcept = default;
};

}  // namespace omnimalloc

namespace std {
template <>
struct hash<omnimalloc::BestFitAllocator> {
  size_t operator()(const omnimalloc::BestFitAllocator&) const noexcept;
};
}  // namespace std
