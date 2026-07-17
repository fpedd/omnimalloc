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
// entry equals the placement's peak. With `clique_cap`, entries are
// additionally capped by their conflict clique's total size — elementwise
// tighter, but the max-equals-peak identity no longer holds. Throws unless
// every allocation is placed.
[[nodiscard]] std::vector<int64_t> per_allocation_placement_pressure(
    const std::vector<Allocation>& allocations, bool clique_cap = false);

}  // namespace omnimalloc
