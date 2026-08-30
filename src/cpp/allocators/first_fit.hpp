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

// Occupied (offset, end) span of a placed allocation, matching the span
// shape that `first_fit_offset` consumes; kUnplaced marks an unplaced one
// (offsets are validated non-negative).
using Interval = std::pair<int64_t, int64_t>;
inline constexpr Interval kUnplaced{-1, -1};

// LSD radix sort by offset (the end rides along as payload; equal-offset order
// is irrelevant to the gap scans). Replaces the comparison sort that dominated
// first-fit at scale; pass count scales with the actual offset magnitude.
void sort_intervals_by_lo(std::vector<Interval>& intervals,
                          std::vector<Interval>& scratch);

// Occupied spans of the already-placed neighbors of one allocation, sorted by
// offset so the gap scans can go left-to-right; `scratch` backs the sort.
template <typename Neighbors>
void gather_placed_spans(const Neighbors& neighbors,
                         const std::vector<Interval>& placed,
                         std::vector<Interval>& spans,
                         std::vector<Interval>& scratch) {
  spans.clear();
  for (const auto neighbor : neighbors) {
    const Interval span = placed[static_cast<size_t>(neighbor)];
    if (span.first >= 0) {
      spans.push_back(span);
    }
  }
  sort_intervals_by_lo(spans, scratch);
}

// First-fit: lowest offset where `size` fits between the sorted spans
[[nodiscard]] int64_t first_fit_offset(int64_t size,
                                       const std::vector<Interval>& spans);

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
// give up mid-run). Adjacency reuse across many orders is FirstFitPlacer's job.
[[nodiscard]] std::vector<Allocation> first_fit_place(
    const std::vector<Allocation>& allocations);

// Greedily place allocations in order using first-fit over the CSR conflict
// adjacency (the fast path: each step only visits the allocation's neighbors)
[[nodiscard]] std::vector<Allocation> first_fit_place_indexed(
    const std::vector<Allocation>& allocations, const CsrAdjacency& adj);

// Shared placement skeleton of the first-fit and best-fit placers: place in
// index order, choosing each offset with `choose_offset` over the sorted spans
// of already-placed neighbors. Seeding pins makes them obstacles throughout.
template <typename OffsetFn>
[[nodiscard]] std::vector<Allocation> place_indexed(
    const std::vector<Allocation>& allocations, const CsrAdjacency& adj,
    OffsetFn choose_offset) {
  check_total_size(allocations);
  const size_t n = allocations.size();
  std::vector<Interval> placed(n, kUnplaced);
  for (size_t i = 0; i < n; ++i) {
    if (const std::optional<int64_t> pin = allocations[i].offset()) {
      placed[i] = {*pin, *pin + allocations[i].size()};
    }
  }
  std::vector<Interval> spans;
  std::vector<Interval> scratch;
  std::vector<Allocation> result;
  result.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    if (placed[i].first < 0) {
      gather_placed_spans(adj.row(i), placed, spans, scratch);
      const int64_t size = allocations[i].size();
      const int64_t offset = choose_offset(size, spans);
      placed[i] = {offset, offset + size};
    }
    result.push_back(allocations[i].with_offset(placed[i].first));
  }
  return result;
}

// Resident first-fit placer for the order-search allocators (genetic, random,
// hill-climb): owns the allocations, their CSR adjacency, and flat size/pin
// snapshots, so placing many candidate orders passes only an index permutation
// across the Python boundary.
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

  // The resident CSR adjacency, for the local searches' inner loops.
  [[nodiscard]] const CsrAdjacency& adjacency() const noexcept { return adj_; }

 private:
  // Throw std::invalid_argument unless every index in `order` is in range
  // and no index repeats.
  void check_order(const std::vector<size_t>& order) const;

  // Placed spans (indexed like allocations_) of a first-fit placement in
  // `order`; assumes `order` has been checked.
  [[nodiscard]] std::vector<Interval> place_spans(
      const std::vector<size_t>& order) const;

  std::vector<Allocation> allocations_;
  CsrAdjacency adj_;
  std::vector<int64_t> sizes_;
  std::vector<int64_t> pins_;  // -1 marks a free allocation
};

}  // namespace omnimalloc
