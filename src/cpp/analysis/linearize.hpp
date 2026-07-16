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

// Surrogate scalar (start, end) times preserving the happens-before conflict
// relation exactly, aligned with `allocations`; scalar input passes through
// unchanged. nullopt when the order is provably not an interval order — or,
// under a set `work_budget` (nullopt means unbounded), when deciding would
// exceed the budget (undecided; the caller falls back to the vector conflict
// engine).
[[nodiscard]] std::optional<std::vector<std::pair<int64_t, int64_t>>>
linearize_times(const std::vector<Allocation>& allocations,
                std::optional<uint64_t> work_budget);

// Allocation-level wrapper: allocations rebuilt with the surrogate scalar
// times, or nullopt when `linearize_times` yields none (not an interval
// order — or undecided under a set `work_budget`).
[[nodiscard]] std::optional<std::vector<Allocation>> try_linearize(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget);

}  // namespace omnimalloc
