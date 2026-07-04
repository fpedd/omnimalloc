//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

class GreedyAllocator {
 public:
  GreedyAllocator() = default;

  // Allocate the given allocations using a first-fit greedy strategy
  std::vector<Allocation> allocate(
      const std::vector<Allocation>& allocations) const;

  bool operator==(const GreedyAllocator&) const noexcept = default;
};

}  // namespace omnimalloc

namespace std {
template <>
struct hash<omnimalloc::GreedyAllocator> {
  size_t operator()(const omnimalloc::GreedyAllocator&) const noexcept;
};
}  // namespace std
