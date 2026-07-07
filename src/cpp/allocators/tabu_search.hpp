//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Neighborhood size, iteration budget, and tabu memory for
// `TabuSearchAllocator`.
struct TabuSearchConfig {
  uint64_t seed = 42;
  int max_iterations = 500;
  int neighborhood_size = 20;  // candidate swaps sampled per iteration
  int tabu_tenure = 15;        // iterations a reversed swap stays forbidden
  // Wall-clock budget checked once per iteration; 0 disables it. Each
  // iteration evaluates `neighborhood_size` full O(n) placements, so
  // `max_iterations` alone does not bound runtime as `allocations` grows -
  // this does.
  double max_seconds = 2.0;
};

// Tabu search over first-fit placement orders. Each iteration samples
// `neighborhood_size` candidate swaps between a currently-peak allocation and
// an earlier temporal neighbor, and moves to the best-scoring candidate that
// is not tabu (or, per the aspiration criterion, a tabu candidate that beats
// the best solution found so far). The swap just made is then forbidden from
// being immediately reversed for `tabu_tenure` iterations, which lets the
// search climb out of local optima without cycling between the same two
// orders. Runs entirely in C++ for the same reason as
// `SimulatedAnnealingAllocator`: no Python round trip per candidate.
class TabuSearchAllocator {
 public:
  explicit TabuSearchAllocator(TabuSearchConfig config = TabuSearchConfig{});

  [[nodiscard]] std::vector<Allocation> allocate(
      const std::vector<Allocation>& allocations) const;

 private:
  TabuSearchConfig config_;
};

}  // namespace omnimalloc
