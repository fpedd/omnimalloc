//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <random>
#include <utility>
#include <vector>

#include "first_fit.hpp"
#include "primitives/allocation.hpp"

namespace omnimalloc {

// Peak memory (highest end offset) across the placed allocations
[[nodiscard]] int64_t peak_of(const std::vector<Allocation>& placed);

// Positions in `placed` whose allocation tops out at `peak`: the move
// candidates for the peak-lowering local searches.
[[nodiscard]] std::vector<size_t> peak_positions(
    const std::vector<Allocation>& placed, int64_t peak);

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

// One random peak-lowering move for the local searches: a random position
// among `peaks` paired with a random earlier temporal neighbor, or nullopt
// when the chosen target has no earlier position to swap with.
[[nodiscard]] std::optional<std::pair<size_t, size_t>> propose_peak_swap(
    const std::vector<size_t>& peaks, const std::vector<size_t>& order,
    const std::vector<Allocation>& allocations,
    const TemporalOverlaps& overlaps, std::mt19937_64& rng);

}  // namespace omnimalloc
