//
// SPDX-License-Identifier: Apache-2.0
//

#include "local_search.hpp"

#include <algorithm>
#include <numeric>

namespace omnimalloc {

int64_t peak_of(const std::vector<Allocation>& placed) {
  int64_t peak = 0;
  for (const auto& alloc : placed) {
    if (const auto height = alloc.height()) {
      peak = std::max(peak, *height);
    }
  }
  return peak;
}

std::vector<size_t> peak_positions(const std::vector<Allocation>& placed,
                                   int64_t peak) {
  std::vector<size_t> positions;
  for (size_t pos = 0; pos < placed.size(); ++pos) {
    const auto height = placed[pos].height();
    if (height && *height == peak) {
      positions.push_back(pos);
    }
  }
  return positions;
}

std::vector<size_t> initial_order(const std::vector<Allocation>& allocations) {
  std::vector<size_t> order(allocations.size());
  std::iota(order.begin(), order.end(), size_t{0});
  std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
    return allocations[a].size() > allocations[b].size();
  });
  return order;
}

std::vector<size_t> earlier_neighbors(
    const std::vector<size_t>& order, size_t target_pos,
    const std::vector<Allocation>& allocations, const ConflictMap& conflicts) {
  std::vector<size_t> neighbors;
  auto it = conflicts.find(allocations[order[target_pos]].id());
  if (it != conflicts.end()) {
    for (size_t pos = 0; pos < target_pos; ++pos) {
      if (it->second.count(allocations[order[pos]].id())) {
        neighbors.push_back(pos);
      }
    }
  }
  if (neighbors.empty()) {
    neighbors.resize(target_pos);
    std::iota(neighbors.begin(), neighbors.end(), size_t{0});
  }
  return neighbors;
}

std::optional<std::pair<size_t, size_t>> propose_peak_swap(
    const std::vector<size_t>& peaks, const std::vector<size_t>& order,
    const std::vector<Allocation>& allocations, const ConflictMap& conflicts,
    std::mt19937_64& rng) {
  std::uniform_int_distribution<size_t> pick_peak(0, peaks.size() - 1);
  const size_t target_pos = peaks[pick_peak(rng)];
  const std::vector<size_t> neighbors =
      earlier_neighbors(order, target_pos, allocations, conflicts);
  if (neighbors.empty()) {
    return std::nullopt;
  }
  std::uniform_int_distribution<size_t> pick_neighbor(0, neighbors.size() - 1);
  return std::make_pair(target_pos, neighbors[pick_neighbor(rng)]);
}

}  // namespace omnimalloc
