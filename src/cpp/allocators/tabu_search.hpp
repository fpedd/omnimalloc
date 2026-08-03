//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Neighborhood size, iteration budget, and tabu memory for
// `tabu_search_place`. Policy defaults live on the Python
// `TabuSearchAllocator`; every field crosses the boundary explicitly.
struct TabuSearchConfig {
  uint64_t seed{};
  int max_iterations{};
  int neighborhood_size{};  // candidate swaps sampled per iteration
  int tabu_tenure{};        // iterations a reversed swap stays forbidden
  // Wall-clock budget checked once per iteration; nullopt disables it. Each
  // iteration evaluates `neighborhood_size` full O(n) placements, so only this
  // bounds runtime as `allocations` grows, never `max_iterations` alone.
  std::optional<double> timeout;
};

// Tabu search over first-fit placement orders: each iteration takes the best
// non-tabu swap among `neighborhood_size` candidates (aspiration admits a tabu
// one that beats the incumbent), forbidding its reversal for `tabu_tenure`.
[[nodiscard]] std::vector<Allocation> tabu_search_place(
    const std::vector<Allocation>& allocations, const TabuSearchConfig& config);

}  // namespace omnimalloc
