//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Generalized greedy-portfolio allocator for scalar and vector-clock
// lifetimes: linearizes vector time to surrogate scalars when the
// happens-before order allows (budgeted), otherwise places truthfully on the
// vector conflict graph. Either way the winning first-fit order of the
// 7-order portfolio decides the offsets.
class OmniAllocator {
 public:
  OmniAllocator() = default;

  std::vector<Allocation> allocate(
      const std::vector<Allocation>& allocations) const;

  bool operator==(const OmniAllocator&) const noexcept = default;
};

}  // namespace omnimalloc

namespace std {
template <>
struct hash<omnimalloc::OmniAllocator> {
  size_t operator()(const omnimalloc::OmniAllocator&) const noexcept;
};
}  // namespace std
