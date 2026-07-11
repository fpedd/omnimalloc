//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Placement policy for the chronological sweep allocator.
enum class SweepFit : std::uint8_t {
  kFirst,     // lowest-offset gap that fits (address-ordered first-fit)
  kBest,      // smallest gap that fits (ties broken by lowest offset)
  kTwoEnded,  // sizes >= median use first-fit, smaller ones best-fit
};

// Chronological sweep placement: allocation/free events are processed in time
// order while an address-ordered, coalescing free list tracks available
// offsets. Same-time events free before allocating; same-time allocations are
// placed largest first. O(N (log N + G)), G = concurrent free gaps.
[[nodiscard]] std::vector<Allocation> sweep_place(
    const std::vector<Allocation>& allocations, SweepFit fit);

// Portfolio placement: the first `num_obstacles` allocations (already in the
// desired placement order) are placed with exact first-fit; the remaining
// allocations are swept chronologically around them, treating the obstacle
// placements as forbidden offset bands while temporally overlapping.
[[nodiscard]] std::vector<Allocation> hybrid_sweep_place(
    const std::vector<Allocation>& allocations, size_t num_obstacles);

}  // namespace omnimalloc
