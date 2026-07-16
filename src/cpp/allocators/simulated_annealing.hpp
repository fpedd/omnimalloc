//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Cooling schedule and iteration budget for `SimulatedAnnealingAllocator`.
// Defaults live in the Python `SimulatedAnnealingConfig` dataclass; every
// field must be set explicitly.
struct SimulatedAnnealingConfig {
  uint64_t seed{};
  int max_iterations{};
  // Percent memory worsening accepted with probability 1/e at iteration 0;
  // decays geometrically by `cooling_rate` every iteration.
  double initial_temperature{};
  double cooling_rate{};
  // Wall-clock budget checked once per iteration; nullopt disables it. Each
  // iteration re-evaluates a full O(n) placement, so `max_iterations` alone
  // does not bound runtime as `allocations` grows - this does.
  std::optional<double> timeout;
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
  explicit SimulatedAnnealingAllocator(SimulatedAnnealingConfig config);

  [[nodiscard]] std::vector<Allocation> allocate(
      const std::vector<Allocation>& allocations) const;

 private:
  SimulatedAnnealingConfig config_;
};

}  // namespace omnimalloc
