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
// placement's peak. A set `closure_cap` bounds the enumeration, throwing
// std::runtime_error once the closure exceeds it; nullopt enumerates
// unbounded (memory grows with the closure).
[[nodiscard]] int64_t closure_pressure(
    const std::vector<Allocation>& allocations,
    std::optional<size_t> closure_cap);

// Exact realizable peak while each allocation is live, aligned with
// `allocations`: the maximum total size at any join-closure cut where the
// allocation is live (every allocation is live at its own birth cut). Can
// sit elementwise strictly below antichain_pressure_per_allocation, since
// pairwise-concurrent allocations need not share a cut; the maximum entry
// equals closure_pressure. A set `closure_cap` bounds the enumeration,
// throwing std::runtime_error once the closure exceeds it; nullopt
// enumerates unbounded (memory grows with the closure).
[[nodiscard]] std::vector<int64_t> closure_pressure_per_allocation(
    const std::vector<Allocation>& allocations,
    std::optional<size_t> closure_cap);

}  // namespace omnimalloc
