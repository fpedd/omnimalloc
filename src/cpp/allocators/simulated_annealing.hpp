//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <vector>

#include "allocators/defaults.hpp"
#include "primitives/allocation.hpp"

namespace omnimalloc {

// Cooling schedule and iteration budget for `SimulatedAnnealingAllocator`.
struct SimulatedAnnealingConfig {
  uint64_t seed = 42;
  int max_iterations = 3000;
  // Percent memory worsening accepted with probability 1/e at iteration 0;
  // decays geometrically by `cooling_rate` every iteration.
  double initial_temperature = 3.0;
  double cooling_rate = 0.998;
  // Wall-clock budget checked once per iteration; 0 disables it. Each
  // iteration re-evaluates a full O(n) placement, so `max_iterations` alone
  // does not bound runtime as `allocations` grows - this does.
  double max_seconds = kDefaultMaxSeconds;
};

// Simulated annealing over first-fit placement orders. Each iteration swaps a
// currently-peak allocation with an earlier temporal neighbor, accepting the
// swap outright when it does not worsen the peak and otherwise with a
// Metropolis probability that anneals to zero over `max_iterations`. The
// entire search runs natively (no Python round trip per candidate), so it can
// evaluate far more candidate placements per second than an
// equivalent Python-orchestrated local search.
class SimulatedAnnealingAllocator {
 public:
  explicit SimulatedAnnealingAllocator(
      SimulatedAnnealingConfig config = SimulatedAnnealingConfig{});

  [[nodiscard]] std::vector<Allocation> allocate(
      const std::vector<Allocation>& allocations) const;

 private:
  SimulatedAnnealingConfig config_;
};

}  // namespace omnimalloc
