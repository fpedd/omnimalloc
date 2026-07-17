//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Generalized greedy-portfolio placement for scalar and vector-clock
// lifetimes: linearizes vector time to surrogate scalars when the
// happens-before order allows (bounded by `linearize_budget`; nullopt means
// unbounded), otherwise places truthfully on the vector conflict graph.
// Either way the winning first-fit order of the 7-order portfolio decides
// the offsets.
[[nodiscard]] std::vector<Allocation> omni_place(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> linearize_budget);

}  // namespace omnimalloc
