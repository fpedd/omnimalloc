//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "analysis/clock.hpp"
#include "primitives/allocation.hpp"

namespace omnimalloc {

// Occupied [offset, end) address span of a placed allocation
using Interval = std::pair<int64_t, int64_t>;

// Offset for an allocation of `size` among the offset-sorted `spans` of its
// placed conflict neighbors
using ChooseOffset = int64_t (*)(int64_t size, std::span<const Interval> spans);

// First-fit: the lowest offset where `size` fits between the spans
[[nodiscard]] int64_t first_fit_offset(
    int64_t size, std::span<const Interval> spans) noexcept;

// Free (-1) or pinned offset per allocation
[[nodiscard]] std::vector<int64_t> pins_of(
    const std::vector<Allocation>& allocations);

// Offsets aligned with `sizes`: the allocations taken in `order`, each placed
// by `choose_offset`. A non-negative pins[i] fixes i there as an obstacle from
// the first placement on; allocations neither pinned nor in `order` stay -1.
[[nodiscard]] std::vector<int64_t> place_order(
    const CsrAdjacency& adj, const std::vector<int64_t>& sizes,
    const std::vector<int64_t>& pins, const std::vector<size_t>& order,
    ChooseOffset choose_offset);

// The greedy_by_* heuristics, in the order place_portfolio races them
enum class GreedyOrder {
  INPUT,
  SIZE,
  DURATION,
  AREA,
  CONFLICT,
  CONFLICT_SIZE,
  START
};

// Indices of `times` in one heuristic's order: largest key first (earliest
// start first for START), with ties kept in input order
[[nodiscard]] std::vector<size_t> greedy_order(
    const std::vector<Allocation>& times, const CsrAdjacency& adj,
    GreedyOrder which);

// Placement in one greedy order by `choose_offset`; pre-set offsets are pins.
// Computes the conflict relation natively, unbudgeted by design: placement
// kernels never give up mid-run.
[[nodiscard]] std::vector<Allocation> greedy_place(
    const std::vector<Allocation>& allocations, GreedyOrder which,
    ChooseOffset choose_offset = first_fit_offset);

// Offsets, aligned with `allocations`, of the lowest-peak first-fit placement
// over every greedy order, ties to the earlier order. Pre-set offsets are
// pins; `surrogate` adds up to 3 orders from its linearized times.
[[nodiscard]] std::vector<int64_t> place_portfolio(
    const std::vector<Allocation>& allocations, const CsrAdjacency& adj,
    const std::vector<Allocation>* surrogate = nullptr);

// Resident first-fit placer for the order-search allocators: owns the
// allocations and their conflict relation, so placing many candidate orders
// passes only an index permutation across the Python boundary.
class FirstFitPlacer {
 public:
  explicit FirstFitPlacer(std::vector<Allocation> allocations);

  // Peak (highest end offset) of a first-fit placement in `order`; throws
  // std::invalid_argument on out-of-range or repeated order indices.
  [[nodiscard]] int64_t peak(const std::vector<size_t>& order) const;

  // First-fit placement of the allocations taken in `order`, in that order;
  // throws std::invalid_argument on out-of-range or repeated order indices.
  [[nodiscard]] std::vector<Allocation> place(
      const std::vector<size_t>& order) const;

  [[nodiscard]] const CsrAdjacency& adjacency() const noexcept { return adj_; }

 private:
  [[nodiscard]] std::vector<int64_t> checked_offsets(
      const std::vector<size_t>& order) const;

  std::vector<Allocation> allocations_;
  CsrAdjacency adj_;
  std::vector<int64_t> sizes_;
  std::vector<int64_t> pins_;
};

}  // namespace omnimalloc
