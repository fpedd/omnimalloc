//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Exact max-weight antichain (weighted Dilworth): the tightest order-derived
// lower bound on any placement's peak. Interval orders resolve by linearizing,
// partial ones by min flow. For certification, not the 10k+ hot path.
[[nodiscard]] int64_t antichain_pressure(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget);

// Exact per-allocation pressure, aligned with `allocations`: the max-weight
// antichain through each, whose maximum entry equals antichain_pressure.
// Interval orders sweep one linearized window, partial ones pin one flow each.
[[nodiscard]] std::vector<int64_t> antichain_pressure_per_allocation(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget);

}  // namespace omnimalloc
