//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <limits>
#include <optional>
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

// Index-based temporal adjacency: position i -> positions overlapping i
using OverlapIndices = std::vector<std::vector<size_t>>;

// Throw std::overflow_error when the total allocation size exceeds `limit`,
// ruling out signed overflow in the placers' offset/cursor arithmetic
void check_total_size(const std::vector<Allocation>& allocations,
                      int64_t limit = std::numeric_limits<int64_t>::max() / 2);

// Map each allocation id to the ids of temporally overlapping allocations
[[nodiscard]] TemporalOverlaps compute_temporal_overlaps(
    const std::vector<Allocation>& allocations);

// Map each allocation index to the indices of temporally overlapping
// allocations
[[nodiscard]] OverlapIndices compute_overlap_indices(
    const std::vector<Allocation>& allocations);

// Greedily place allocations in order using first-fit, reusing a precomputed
// overlap map
[[nodiscard]] std::vector<Allocation> first_fit_place(
    const std::vector<Allocation>& allocations,
    const TemporalOverlaps& overlaps);

// Greedily place allocations in order using first-fit over an index-based
// adjacency (the fast path: each step only visits the allocation's neighbors)
[[nodiscard]] std::vector<Allocation> first_fit_place_indexed(
    const std::vector<Allocation>& allocations, const OverlapIndices& indices);

// Peak memory (highest end offset) across the placed allocations
[[nodiscard]] int64_t peak_of(const std::vector<Allocation>& placed);

// Indices into `allocations`, sorted largest-size-first: a decent, cheap
// starting order for the order-search allocators.
[[nodiscard]] std::vector<size_t> initial_order(
    const std::vector<Allocation>& allocations);

// Positions before `target_pos` in `order` whose allocation temporally
// overlaps the one at `target_pos`, or every earlier position if none do.
[[nodiscard]] std::vector<size_t> earlier_neighbors(
    const std::vector<size_t>& order, size_t target_pos,
    const std::vector<Allocation>& allocations,
    const TemporalOverlaps& overlaps);

// Resident first-fit placer for the order-search allocators (genetic, random,
// hill-climb): owns the allocations and their overlap maps (computed once) so
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
  // Offsets (indexed like allocations_) of a first-fit placement in `order`.
  [[nodiscard]] std::vector<std::optional<int64_t>> place_offsets(
      const std::vector<size_t>& order) const;

  std::vector<Allocation> allocations_;
  TemporalOverlaps overlaps_;
  OverlapIndices indices_;
};

}  // namespace omnimalloc
