//
// SPDX-License-Identifier: Apache-2.0
//

#include "tabu_search.hpp"

#include <random>
#include <unordered_map>
#include <utility>

#include "first_fit.hpp"
#include "local_search.hpp"

namespace omnimalloc {

namespace {

// Order-independent key for the pair of original allocation indices a swap
// exchanges, used to record/check tabu status.
int64_t tabu_key(size_t a, size_t b, size_t num_allocations) {
  if (a > b) {
    std::swap(a, b);
  }
  return static_cast<int64_t>(a * num_allocations + b);
}

}  // namespace

TabuSearchAllocator::TabuSearchAllocator(TabuSearchConfig config)
    : config_(config) {}

std::vector<Allocation> TabuSearchAllocator::allocate(
    const std::vector<Allocation>& allocations) const {
  const FirstFitPlacer placer(allocations);
  std::vector<size_t> order = initial_order(allocations);
  if (allocations.size() < 2) {
    return placer.place(order);
  }

  const size_t n = allocations.size();

  std::vector<Allocation> current_placed = placer.place(order);
  int64_t current_peak = peak_of(current_placed);
  std::vector<Allocation> best_placed = current_placed;
  int64_t best_peak = current_peak;

  std::mt19937_64 rng(config_.seed);
  std::unordered_map<int64_t, int> tabu_until;

  const auto deadline = make_deadline(config_.timeout);

  for (int iteration = 0; iteration < config_.max_iterations; ++iteration) {
    if (deadline_expired(deadline)) {
      break;
    }

    const std::vector<size_t> peaks =
        peak_positions(current_placed, current_peak);
    if (peaks.empty()) {
      break;  // no placed allocation reaches the peak: nothing to perturb
    }

    // Sample a neighborhood of candidate swaps and keep the best admissible
    // one: non-tabu, or tabu but beating the best-ever solution (aspiration).
    size_t best_p1 = 0;
    size_t best_p2 = 0;
    int64_t best_candidate_peak = -1;
    std::vector<Allocation> best_candidate_placed;
    bool best_is_tabu = false;

    for (int sample = 0; sample < config_.neighborhood_size; ++sample) {
      const auto proposal =
          propose_peak_swap(peaks, order, allocations, placer.overlaps(), rng);
      if (!proposal) {
        continue;
      }
      const auto [target_pos, other_pos] = *proposal;

      auto tabu_it =
          tabu_until.find(tabu_key(order[target_pos], order[other_pos], n));
      bool is_tabu = tabu_it != tabu_until.end() && tabu_it->second > iteration;

      std::swap(order[target_pos], order[other_pos]);
      std::vector<Allocation> candidate_placed = placer.place(order);
      int64_t candidate_peak = peak_of(candidate_placed);
      std::swap(order[target_pos],
                order[other_pos]);  // undo; reapplied if chosen

      bool aspires = candidate_peak < best_peak;
      if ((!is_tabu || aspires) &&
          (best_candidate_peak < 0 || candidate_peak < best_candidate_peak)) {
        best_candidate_peak = candidate_peak;
        best_candidate_placed = std::move(candidate_placed);
        best_p1 = target_pos;
        best_p2 = other_pos;
        best_is_tabu = is_tabu;
      }
    }

    if (best_candidate_peak < 0) {
      continue;  // every sampled move was tabu without meeting aspiration
    }

    std::swap(order[best_p1], order[best_p2]);
    current_placed = std::move(best_candidate_placed);
    current_peak = best_candidate_peak;
    if (!best_is_tabu) {
      tabu_until[tabu_key(order[best_p1], order[best_p2], n)] =
          iteration + config_.tabu_tenure;
    }

    if (current_peak < best_peak) {
      best_placed = current_placed;
      best_peak = current_peak;
    }
  }

  return best_placed;
}

}  // namespace omnimalloc
