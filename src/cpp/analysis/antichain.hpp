//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Exact max-weight antichain of the happens-before order (weighted
// Dilworth): the tightest order-derived lower bound on any placement's
// peak, since pairwise-conflicting allocations must occupy disjoint address
// ranges. Interval orders resolve through linearization and the scalar
// sweep; genuinely partial orders through min flow with per-allocation
// lower bounds. Built to certify allocator optimality at small and medium
// scale, not for the 10k+ hot path. A set `work_budget` (nullopt means
// unbounded) bounds both the linearize attempt and the flow construction
// (dominance-counting work, O(k * m * d) over deduplicated clock rows); the
// flow path throws instead of stalling or exhausting memory once the budget
// is exceeded.
[[nodiscard]] int64_t antichain_pressure(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget);

// Exact per-allocation pressure, aligned with `allocations`: for each
// allocation the max-weight antichain through it, i.e. the heaviest
// pairwise-conflicting set containing it. The elementwise-tightest
// order-derived lower bound on the pressure any placement can exhibit while
// that allocation is live; the maximum entry equals antichain_pressure.
// Interval orders resolve through one linearized window sweep; genuinely
// partial orders solve one pinned min flow per distinct lifetime over its
// conflict neighborhood — built for certification, not the 10k+ hot path.
// A set `work_budget` bounds the linearize attempt and each pinned flow
// (see antichain_pressure); the flow path throws once it is exceeded.
[[nodiscard]] std::vector<int64_t> antichain_pressure_per_allocation(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget);

}  // namespace omnimalloc
