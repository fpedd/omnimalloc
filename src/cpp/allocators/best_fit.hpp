//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Best-fit placement: like first-fit, but among the gaps left by placed
// conflicting allocations it takes the smallest that fits, ties to the lowest
// offset. Falls back to placing after the last one when no finite gap fits.
[[nodiscard]] std::vector<Allocation> best_fit_place(
    const std::vector<Allocation>& allocations);

}  // namespace omnimalloc
