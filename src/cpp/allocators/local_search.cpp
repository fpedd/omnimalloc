//
// SPDX-License-Identifier: Apache-2.0
//

#include "local_search.hpp"

#include <algorithm>
#include <cassert>
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

std::vector<size_t> earlier_neighbors(const std::vector<size_t>& order,
                                      size_t target_pos,
                                      const ConflictIndices& indices) {
  // Mark the target's conflicts, then keep the earlier positions holding
  // one; the marks cost a pass over the order, the alternative a hash
  // lookup per earlier position
  std::vector<char> conflicting(order.size(), 0);
  for (size_t other : indices[order[target_pos]]) {
    conflicting[other] = 1;
  }
  std::vector<size_t> neighbors;
  for (size_t pos = 0; pos < target_pos; ++pos) {
    if (conflicting[order[pos]] != 0) {
      neighbors.push_back(pos);
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
    const ConflictIndices& indices, std::mt19937_64& rng) {
  assert(!peaks.empty());  // full placements always attain their peak
  std::uniform_int_distribution<size_t> pick_peak(0, peaks.size() - 1);
  const size_t target_pos = peaks[pick_peak(rng)];
  const std::vector<size_t> neighbors =
      earlier_neighbors(order, target_pos, indices);
  if (neighbors.empty()) {
    return std::nullopt;
  }
  std::uniform_int_distribution<size_t> pick_neighbor(0, neighbors.size() - 1);
  return std::make_pair(target_pos, neighbors[pick_neighbor(rng)]);
}

}  // namespace omnimalloc
