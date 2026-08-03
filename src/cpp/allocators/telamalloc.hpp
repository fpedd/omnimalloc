//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Search budgets for `telamalloc_place`. Policy defaults live on the Python
// `TelamallocAllocator`; every field crosses the boundary explicitly.
struct TelamallocConfig {
  // Seeds the random-walk step of the conflict repair (see `pack_phase`);
  // results are deterministic for a fixed seed.
  uint64_t seed{};
  // Eviction (backtrack) budget per capacity attempt; an attempt that
  // exhausts it reports the capacity as unreachable.
  int max_backtracks{};
  // Wall-clock budget for the whole placement; nullopt disables it, leaving
  // the per-attempt `max_backtracks` as the only bound.
  std::optional<double> timeout;
};

// TelaMalloc-style placement (Maas et al., ASPLOS 2023), adapted to minimize
// peak memory: pack each conflict-graph component by longest lifetime then
// largest size, with min-conflict eviction, binary-searching its capacity.
[[nodiscard]] std::vector<Allocation> telamalloc_place(
    const std::vector<Allocation>& allocations, const TelamallocConfig& config);

}  // namespace omnimalloc
