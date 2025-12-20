//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "primitives/allocation.hpp"
#include "primitives/id_type.hpp"

namespace omnimalloc {

class GreedyAllocator {
 public:
  GreedyAllocator() = default;

  // Allocate the given allocations using a first-fit greedy strategy
  std::vector<Allocation> allocate(
      const std::vector<Allocation>& allocations) const;

  bool operator==(const GreedyAllocator&) const noexcept = default;

 private:
  static std::unordered_map<IdType, std::unordered_set<IdType, IdTypeHash>,
                            IdTypeHash>
  compute_temporal_overlaps(const std::vector<Allocation>& allocations);

  int64_t find_best_offset(
      const Allocation& current_alloc,
      const std::vector<Allocation>& placed_allocations,
      const std::unordered_map<IdType, std::unordered_set<IdType, IdTypeHash>,
                               IdTypeHash>& overlaps) const;
};

}  // namespace omnimalloc

namespace std {
template <>
struct hash<omnimalloc::GreedyAllocator> {
  size_t operator()(const omnimalloc::GreedyAllocator&) const noexcept;
};
}  // namespace std
