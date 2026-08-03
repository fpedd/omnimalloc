//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "analysis/conflicts.hpp"
#include "primitives/allocation.hpp"

namespace omnimalloc {

// Occupied (offset, end) spans of the already-placed neighbors of one
// allocation, sorted by offset so the gap scans can go left-to-right
void gather_spans(const std::vector<size_t>& neighbors,
                  const std::vector<std::optional<int64_t>>& offsets,
                  const std::vector<Allocation>& allocations,
                  std::vector<std::pair<int64_t, int64_t>>& spans);

// First-fit: lowest offset where `size` fits between the sorted spans
[[nodiscard]] int64_t first_fit_offset(
    int64_t size, const std::vector<std::pair<int64_t, int64_t>>& spans);

// Offsets (aligned with `allocations`) and peak of the winning placement.
struct PortfolioPlacement {
  std::vector<int64_t> offsets;
  int64_t peak = 0;
};

// First-fit over the 7-order greedy portfolio in parallel, keeping the lowest
// peak; ties break by the fixed order sequence. A pre-existing offset pins its
// allocation; `surrogate` appends 3 orders.
[[nodiscard]] PortfolioPlacement place_portfolio(
    const std::vector<Allocation>& allocations, const CsrAdjacency& adj,
    const std::vector<Allocation>* surrogate = nullptr);

// Greedily place allocations in input order using first-fit; computes the
// conflict relation natively (unbudgeted by design: placement kernels never
// give up mid-run). Map reuse across many orders is FirstFitPlacer's job.
[[nodiscard]] std::vector<Allocation> first_fit_place(
    const std::vector<Allocation>& allocations);

// Greedily place allocations in order using first-fit over an index-based
// adjacency (the fast path: each step only visits the allocation's neighbors)
[[nodiscard]] std::vector<Allocation> first_fit_place_indexed(
    const std::vector<Allocation>& allocations, const ConflictIndices& indices);

// Shared placement skeleton of the first-fit and best-fit placers: place in
// index order, choosing each offset with `choose_offset` over the sorted spans
// of already-placed neighbors. Seeding pins makes them obstacles throughout.
template <typename OffsetFn>
[[nodiscard]] std::vector<Allocation> place_indexed(
    const std::vector<Allocation>& allocations, const ConflictIndices& indices,
    OffsetFn choose_offset) {
  check_total_size(allocations);
  std::vector<std::optional<int64_t>> offsets(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    offsets[i] = allocations[i].offset();
  }
  std::vector<std::pair<int64_t, int64_t>> spans;
  std::vector<Allocation> placed;
  placed.reserve(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    if (!allocations[i].offset().has_value()) {
      gather_spans(indices[i], offsets, allocations, spans);
      offsets[i] = choose_offset(allocations[i].size(), spans);
    }
    placed.push_back(allocations[i].with_offset(*offsets[i]));
  }
  return placed;
}

// Resident first-fit placer for the order-search allocators (genetic, random,
// hill-climb): owns the allocations and their conflict maps, so placing many
// candidate orders passes only an index permutation across the Python boundary.
class FirstFitPlacer {
 public:
  explicit FirstFitPlacer(std::vector<Allocation> allocations);

  // Peak memory (highest end offset) of a first-fit placement in `order`;
  // throws std::invalid_argument on out-of-range or repeated order indices.
  [[nodiscard]] int64_t peak(const std::vector<size_t>& order) const;

  // First-fit placement of the allocations taken in `order`, in that order;
  // throws std::invalid_argument on out-of-range or repeated order indices.
  [[nodiscard]] std::vector<Allocation> place(
      const std::vector<size_t>& order) const;

  // The resident index adjacency, for the local searches' inner loops.
  [[nodiscard]] const ConflictIndices& indices() const noexcept {
    return indices_;
  }

 private:
  // Throw std::invalid_argument unless every index in `order` is in range
  // and no index repeats.
  void check_order(const std::vector<size_t>& order) const;

  // Offsets (indexed like allocations_) of a first-fit placement in `order`;
  // assumes `order` has been checked.
  [[nodiscard]] std::vector<std::optional<int64_t>> place_offsets(
      const std::vector<size_t>& order) const;

  std::vector<Allocation> allocations_;
  ConflictIndices indices_;
};

}  // namespace omnimalloc
