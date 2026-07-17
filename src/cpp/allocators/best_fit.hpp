//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Best-fit placement: like first-fit, but among the gaps left by
// already-placed conflicting allocations it picks the smallest one that fits
// (ties broken by lowest offset) rather than the first one found. Falls back
// to placing after the last conflicting allocation when no finite gap fits.
[[nodiscard]] std::vector<Allocation> best_fit_place(
    const std::vector<Allocation>& allocations);

}  // namespace omnimalloc
