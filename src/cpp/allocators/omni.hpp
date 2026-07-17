//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Generalized greedy-portfolio allocator for scalar and vector-clock
// lifetimes: linearizes vector time to surrogate scalars when the
// happens-before order allows (bounded by `linearize_budget`; nullopt means
// unbounded), otherwise places truthfully on the vector conflict graph.
// Either way the winning first-fit order of the 7-order portfolio decides
// the offsets.
class OmniAllocator {
 public:
  explicit OmniAllocator(std::optional<uint64_t> linearize_budget)
      : linearize_budget_(linearize_budget) {}

  std::vector<Allocation> allocate(
      const std::vector<Allocation>& allocations) const;

  [[nodiscard]] std::optional<uint64_t> linearize_budget() const noexcept {
    return linearize_budget_;
  }

  bool operator==(const OmniAllocator&) const noexcept = default;

 private:
  std::optional<uint64_t> linearize_budget_;
};

}  // namespace omnimalloc

namespace std {
template <>
struct hash<omnimalloc::OmniAllocator> {
  size_t operator()(const omnimalloc::OmniAllocator&) const noexcept;
};
}  // namespace std
