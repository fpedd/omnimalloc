//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Cooling schedule and iteration budget for `simulated_annealing_place`.
// Policy defaults live on the Python `SimulatedAnnealingAllocator`; every
// field crosses the boundary explicitly.
struct SimulatedAnnealingConfig {
  uint64_t seed{};
  int max_iterations{};
  // Percent memory worsening accepted with probability 1/e at iteration 0;
  // decays geometrically by `cooling_rate` every iteration.
  double initial_temperature{};
  double cooling_rate{};
  // Wall-clock budget checked once per iteration; nullopt disables it. Each
  // iteration re-evaluates a full O(n) placement, so only this bounds runtime
  // as `allocations` grows, never `max_iterations` alone.
  std::optional<double> timeout;
};

// Simulated annealing over first-fit placement orders: each iteration swaps a
// currently-peak allocation with an earlier temporal neighbor, taking it
// outright unless it worsens the peak, else with Metropolis probability.
[[nodiscard]] std::vector<Allocation> simulated_annealing_place(
    const std::vector<Allocation>& allocations,
    const SimulatedAnnealingConfig& config);

}  // namespace omnimalloc
