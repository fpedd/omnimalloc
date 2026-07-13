//
// SPDX-License-Identifier: Apache-2.0
//

#include "greedy.hpp"

#include "greedy_base.hpp"

namespace omnimalloc {

std::vector<Allocation> GreedyAllocator::allocate(
    const std::vector<Allocation>& allocations) const {
  return first_fit_place_indexed(allocations,
                                 compute_overlap_indices(allocations));
}

}  // namespace omnimalloc

namespace std {

size_t hash<omnimalloc::GreedyAllocator>::operator()(
    const omnimalloc::GreedyAllocator&) const noexcept {
  // Stateless class - all instances are equal, use constant hash
  return 0x9e3779b9;  // arbitrary constant
}

}  // namespace std
