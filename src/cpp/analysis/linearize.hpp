//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Surrogate scalar (start, end) times preserving the happens-before relation
// exactly, aligned with `allocations`. nullopt when the order is provably not
// an interval order, or, with `undecided` set, when `work_budget` ran out.
[[nodiscard]] std::optional<std::vector<std::pair<int64_t, int64_t>>>
linearize_times(const std::vector<Allocation>& allocations,
                std::optional<uint64_t> work_budget, bool* undecided = nullptr);

// Allocation-level wrapper: allocations rebuilt with the surrogate scalar
// times, or nullopt when `linearize_times` yields none.
[[nodiscard]] std::optional<std::vector<Allocation>> try_linearize(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget, bool* undecided = nullptr);

}  // namespace omnimalloc
