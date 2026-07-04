//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "primitives/allocation.hpp"
#include "primitives/id_type.hpp"

namespace omnimalloc {

// Temporal overlap adjacency: allocation id -> ids of overlapping allocations
using TemporalOverlaps =
    std::unordered_map<IdType, std::unordered_set<IdType, IdTypeHash>,
                       IdTypeHash>;

// Map each allocation id to the ids of temporally overlapping allocations
[[nodiscard]] TemporalOverlaps compute_temporal_overlaps(
    const std::vector<Allocation>& allocations);

// Greedily place allocations in order using first-fit, reusing a precomputed
// overlap map
[[nodiscard]] std::vector<Allocation> first_fit_place(
    const std::vector<Allocation>& allocations,
    const TemporalOverlaps& overlaps);

// Resident first-fit placer for the order-search allocators (genetic, random,
// hill-climb): owns the allocations and their overlap map (computed once) so
// that placing many candidate orderings only passes an index permutation across
// the Python boundary, never re-marshaling the loop-invariant overlap map.
class FirstFitPlacer {
 public:
  explicit FirstFitPlacer(std::vector<Allocation> allocations);

  // Peak memory (highest end offset) of a first-fit placement in `order`.
  [[nodiscard]] int64_t evaluate(const std::vector<size_t>& order) const;

  // First-fit placement of the allocations taken in `order`, in that order.
  [[nodiscard]] std::vector<Allocation> place(
      const std::vector<size_t>& order) const;

  // The resident overlap map, keyed by allocation id.
  [[nodiscard]] const TemporalOverlaps& overlaps() const noexcept {
    return overlaps_;
  }

 private:
  std::vector<Allocation> allocations_;
  TemporalOverlaps overlaps_;
};

}  // namespace omnimalloc
