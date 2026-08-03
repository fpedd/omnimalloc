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

// Exact realizable peak: the maximum total size jointly live at a single cut,
// scored over the join-closure of the birth clocks. Pairwise-concurrent
// allocations need not share a cut, so this can sit below antichain_pressure.
[[nodiscard]] int64_t closure_pressure(
    const std::vector<Allocation>& allocations,
    std::optional<size_t> closure_cap);

// Exact realizable peak while each allocation is live, aligned with
// `allocations`: the maximum total size at any join-closure cut where it is
// live. The maximum entry equals closure_pressure. `closure_cap` bounds it.
[[nodiscard]] std::vector<int64_t> closure_pressure_per_allocation(
    const std::vector<Allocation>& allocations,
    std::optional<size_t> closure_cap);

}  // namespace omnimalloc
