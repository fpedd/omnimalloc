//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
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

// Occupied (offset, end) spans of the already-placed neighbors of one
// allocation, sorted by offset so the gap scans can go left-to-right
void gather_spans(const std::vector<size_t>& neighbors,
                  const std::vector<std::optional<int64_t>>& offsets,
                  const std::vector<Allocation>& allocations,
                  std::vector<std::pair<int64_t, int64_t>>& spans);

// First-fit: lowest offset where `size` fits between the sorted spans
[[nodiscard]] int64_t first_fit_offset(
    int64_t size, const std::vector<std::pair<int64_t, int64_t>>& spans);

// Per-allocation count of temporally overlapping allocations, aligned with
// `allocations`. Counts with multiplicity, so duplicate ids stay distinct.
[[nodiscard]] std::vector<int64_t> compute_conflict_degrees(
    const std::vector<Allocation>& allocations);

// Greedy first-fit sort orders for compute_allocation_peaks, mirroring the
// greedy_by_* allocators; kAll runs the whole portfolio in parallel and keeps
// the placement with the lowest peak.
enum class GreedyOrder : std::uint8_t {
  kInput,
  kSize,
  kDuration,
  kArea,
  kConflict,
  kConflictSize,
  kStart,
  kAll,
};

// Per-allocation BOUND peaks, aligned with `allocations`: place everything
// with the selected greedy first-fit order (pre-existing offsets are
// ignored), then reduce each allocation to the min of its closed
// conflict-neighborhood weight sum and the placement skyline (highest end
// offset across that neighborhood).
[[nodiscard]] std::vector<int64_t> compute_allocation_peaks(
    const std::vector<Allocation>& allocations,
    GreedyOrder order = GreedyOrder::kAll);

// Greedily place allocations in order using first-fit, reusing a precomputed
// overlap map
[[nodiscard]] std::vector<Allocation> first_fit_place(
    const std::vector<Allocation>& allocations,
    const TemporalOverlaps& overlaps);

// Greedily place allocations in order using first-fit over an index-based
// adjacency (the fast path: each step only visits the allocation's neighbors)
[[nodiscard]] std::vector<Allocation> first_fit_place_indexed(
    const std::vector<Allocation>& allocations, const OverlapIndices& indices);

// Shared placement skeleton of the first-fit and best-fit placers: place
// allocations in index order, choosing each offset with `choose_offset` over
// the sorted spans of the allocation's already-placed neighbors
template <typename OffsetFn>
[[nodiscard]] std::vector<Allocation> place_indexed(
    const std::vector<Allocation>& allocations, const OverlapIndices& indices,
    OffsetFn choose_offset) {
  check_total_size(allocations);
  std::vector<std::optional<int64_t>> offsets(allocations.size());
  std::vector<std::pair<int64_t, int64_t>> spans;
  std::vector<Allocation> placed;
  placed.reserve(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    gather_spans(indices[i], offsets, allocations, spans);
    offsets[i] = choose_offset(allocations[i].size(), spans);
    placed.push_back(allocations[i].with_offset(*offsets[i]));
  }
  return placed;
}

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
