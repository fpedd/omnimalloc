//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Placement-certified per-allocation pressure, aligned with `allocations`:
// the highest occupied address among each allocation and its conflict
// neighbors, an upper bound on the pressure while it is live whose maximum
// entry equals the placement's peak. Throws unless every allocation is
// placed.
[[nodiscard]] std::vector<int64_t> placement_pressure_per_allocation(
    const std::vector<Allocation>& allocations);

}  // namespace omnimalloc
