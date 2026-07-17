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
  // iteration evaluates `neighborhood_size` full O(n) placements, so
  // `max_iterations` alone does not bound runtime as `allocations` grows -
  // this does.
  std::optional<double> timeout;
};

// Tabu search over first-fit placement orders. Each iteration samples
// `neighborhood_size` candidate swaps between a currently-peak allocation and
// an earlier temporal neighbor, and moves to the best-scoring candidate that
// is not tabu (or, per the aspiration criterion, a tabu candidate that beats
// the best solution found so far). The swap just made is then forbidden from
// being immediately reversed for `tabu_tenure` iterations, which lets the
// search climb out of local optima without cycling between the same two
// orders. Runs entirely in C++ for the same reason as
// `simulated_annealing_place`: no Python round trip per candidate.
[[nodiscard]] std::vector<Allocation> tabu_search_place(
    const std::vector<Allocation>& allocations, const TabuSearchConfig& config);

}  // namespace omnimalloc
