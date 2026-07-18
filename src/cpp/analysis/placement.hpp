//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Placement-certified per-allocation pressure, aligned with `allocations`:
// the highest occupied address among each allocation and its conflict
// neighbors, an upper bound on the pressure while it is live whose maximum
// entry equals the placement's peak. Throws unless every allocation is
// placed. A set `work_budget` (nullopt means unbounded) bounds both the
// linearize attempt and the fallback conflict sweep; past it, throw instead
// of stalling.
[[nodiscard]] std::vector<int64_t> placement_pressure_per_allocation(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget);

}  // namespace omnimalloc
