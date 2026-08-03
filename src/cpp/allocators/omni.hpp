//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Generalized greedy-portfolio placement: linearizes vector time to surrogate
// scalars when the order allows (`linearize_budget`), else places on the vector
// conflict graph. Seven orders race, plus three from any linearization.
[[nodiscard]] std::vector<Allocation> omni_place(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> linearize_budget);

}  // namespace omnimalloc
