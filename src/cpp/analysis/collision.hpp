//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Input indices of the lexicographically first colliding pair (allocations
// that conflict in time yet share addresses), or nullopt when the placement is
// sound. Throws unless all are placed and the clock dimensions agree.
[[nodiscard]] std::optional<std::pair<size_t, size_t>> find_collision(
    const std::vector<Allocation>& allocations);

}  // namespace omnimalloc
