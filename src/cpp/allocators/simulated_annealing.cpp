//
// SPDX-License-Identifier: Apache-2.0
//

#include "simulated_annealing.hpp"

#include <chrono>
#include <cmath>
#include <random>
#include <utility>

#include "greedy_base.hpp"

namespace omnimalloc {

SimulatedAnnealingAllocator::SimulatedAnnealingAllocator(
    SimulatedAnnealingConfig config)
    : config_(config) {}

std::vector<Allocation> SimulatedAnnealingAllocator::allocate(
    const std::vector<Allocation>& allocations) const {
  const FirstFitPlacer placer(allocations);
  std::vector<size_t> order = initial_order(allocations);
  if (allocations.size() < 2) {
    return placer.place(order);
  }

  std::vector<Allocation> current_placed = placer.place(order);
  int64_t current_peak = peak_of(current_placed);
  std::vector<Allocation> best_placed = current_placed;
  int64_t best_peak = current_peak;

  std::mt19937_64 rng(config_.seed);
  std::uniform_real_distribution<double> unit(0.0, 1.0);
  double temperature = config_.initial_temperature;

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::duration<double>(config_.max_seconds);

  for (int iteration = 0; iteration < config_.max_iterations; ++iteration) {
    if (config_.max_seconds > 0.0 &&
        std::chrono::steady_clock::now() >= deadline) {
      break;
    }

    std::vector<size_t> peak_positions;
    for (size_t pos = 0; pos < current_placed.size(); ++pos) {
      const auto height = current_placed[pos].height();
      if (height && *height == current_peak) {
        peak_positions.push_back(pos);
      }
    }
    if (peak_positions.empty()) {
      break;  // no placed allocation reaches the peak: nothing to perturb
    }

    std::uniform_int_distribution<size_t> pick_peak(0,
                                                    peak_positions.size() - 1);
    size_t target_pos = peak_positions[pick_peak(rng)];
    std::vector<size_t> neighbors =
        earlier_neighbors(order, target_pos, allocations, placer.overlaps());
    if (neighbors.empty()) {
      temperature *= config_.cooling_rate;
      continue;
    }
    std::uniform_int_distribution<size_t> pick_neighbor(0,
                                                        neighbors.size() - 1);
    size_t other_pos = neighbors[pick_neighbor(rng)];

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

    temperature *= config_.cooling_rate;
  }

  return best_placed;
}

}  // namespace omnimalloc
