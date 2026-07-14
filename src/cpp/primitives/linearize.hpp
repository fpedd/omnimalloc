//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "allocation.hpp"

namespace omnimalloc {

inline constexpr uint64_t kNoLinearizeBudget =
    std::numeric_limits<uint64_t>::max();

// Default dominance-counting budget for implicit and hot-path callers (the
// omni allocator's linearize attempt, `Pool.pressure`), so huge vector-clock
// instances fail fast instead of stalling or exhausting memory. Exported to
// Python as DEFAULT_WORK_BUDGET.
inline constexpr uint64_t kDefaultWorkBudget = 100'000'000;

// Surrogate scalar (start, end) times preserving the happens-before conflict
// relation exactly, aligned with `allocations`; scalar input passes through
// unchanged. nullopt when the order is provably not an interval order — or,
// under a finite `work_budget`, when deciding would exceed the budget
// (undecided; the caller falls back to the vector conflict engine).
[[nodiscard]] std::optional<std::vector<std::pair<int64_t, int64_t>>>
linearize_times(const std::vector<Allocation>& allocations,
                uint64_t work_budget = kNoLinearizeBudget);

// Allocation-level wrapper: allocations rebuilt with the surrogate scalar
// times, or nullopt when `linearize_times` yields none (not an interval
// order — or undecided under a finite `work_budget`).
[[nodiscard]] std::optional<std::vector<Allocation>> try_linearize(
    const std::vector<Allocation>& allocations,
    uint64_t work_budget = kNoLinearizeBudget);

}  // namespace omnimalloc
