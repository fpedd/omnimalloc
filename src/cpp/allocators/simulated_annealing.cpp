//
// SPDX-License-Identifier: Apache-2.0
//

#include "simulated_annealing.hpp"

#include <cmath>
#include <random>
#include <utility>

#include "common/deadline.hpp"
#include "first_fit.hpp"
#include "local_search.hpp"

namespace omnimalloc {

std::vector<Allocation> simulated_annealing_place(
    const std::vector<Allocation>& allocations,
    const SimulatedAnnealingConfig& config) {
  const FirstFitPlacer placer(allocations);
  std::vector<size_t> order = initial_order(allocations);
  if (allocations.size() < 2) {
    return placer.place(order);
  }

  std::vector<Allocation> current_placed = placer.place(order);
  int64_t current_peak = peak_of(current_placed);
  std::vector<Allocation> best_placed = current_placed;
  int64_t best_peak = current_peak;

  std::mt19937_64 rng(config.seed);
  std::uniform_real_distribution<double> unit(0.0, 1.0);
  double temperature = config.initial_temperature;

  const auto deadline = make_deadline(config.timeout);

  for (int iteration = 0; iteration < config.max_iterations; ++iteration) {
    if (deadline_expired(deadline)) {
      break;
    }

    const std::vector<size_t> peaks =
        peak_positions(current_placed, current_peak);
    if (peaks.empty()) {
      break;  // no placed allocation reaches the peak: nothing to perturb
    }

    const auto proposal =
        propose_peak_swap(peaks, order, allocations, placer.conflicts(), rng);
    if (!proposal) {
      temperature *= config.cooling_rate;
      continue;
    }
    const auto [target_pos, other_pos] = *proposal;

    std::swap(order[target_pos], order[other_pos]);
    std::vector<Allocation> candidate_placed = placer.place(order);
    int64_t candidate_peak = peak_of(candidate_placed);

    bool accept = candidate_peak <= current_peak;
    if (!accept && current_peak > 0 && temperature > 0.0) {
      double worsening_percent =
          100.0 * static_cast<double>(candidate_peak - current_peak) /
          static_cast<double>(current_peak);
      accept = unit(rng) < std::exp(-worsening_percent / temperature);
    }

    if (accept) {
      current_placed = std::move(candidate_placed);
      current_peak = candidate_peak;
      if (current_peak < best_peak) {
        best_placed = current_placed;
        best_peak = current_peak;
      }
    } else {
      std::swap(order[target_pos], order[other_pos]);  // undo the rejected swap
    }

    temperature *= config.cooling_rate;
  }

  return best_placed;
}

}  // namespace omnimalloc
