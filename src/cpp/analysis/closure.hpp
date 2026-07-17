//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Exact realizable peak: the maximum total size jointly live at a single
// cut, scored over the join-closure of the birth clocks (the minimal cut
// where a candidate set is jointly live is the join of its births, and
// joins of observed clocks span the consistent-cut lattice). Note that
// pairwise-concurrent allocations need not share a cut, so this can sit
// strictly below antichain_pressure; both soundly lower-bound any
// placement's peak. nullopt once the closure exceeds `closure_cap`.
[[nodiscard]] std::optional<int64_t> closure_pressure(
    const std::vector<Allocation>& allocations, size_t closure_cap);

// Exact realizable peak while each allocation is live, aligned with
// `allocations`: the maximum total size at any join-closure cut where the
// allocation is live (every allocation is live at its own birth cut). Can
// sit elementwise strictly below per_allocation_antichain_pressure, since
// pairwise-concurrent allocations need not share a cut; the maximum entry
// equals closure_pressure. nullopt once the closure exceeds `closure_cap`.
[[nodiscard]] std::optional<std::vector<int64_t>>
per_allocation_closure_pressure(const std::vector<Allocation>& allocations,
                                size_t closure_cap);

}  // namespace omnimalloc
