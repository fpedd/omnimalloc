//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <vector>

#include "allocators/defaults.hpp"
#include "primitives/allocation.hpp"

namespace omnimalloc {

// Search budgets for `TelamallocAllocator`.
struct TelamallocConfig {
  // Seeds the random-walk step of the conflict repair (see `pack_phase`);
  // results are deterministic for a fixed seed.
  uint64_t seed = 42;
  // Eviction (backtrack) budget per capacity attempt; an attempt that
  // exhausts it reports the capacity as unreachable.
  int max_backtracks = 10000;
  // Wall-clock budget for the whole allocate(); 0 disables it, leaving the
  // per-attempt `max_backtracks` as the only bound.
  double timeout = kDefaultTimeout;
};

// TelaMalloc-style allocator after Maas et al., ASPLOS 2023 ("TelaMalloc:
// Efficient On-Chip Memory Allocation for Production Machine Learning
// Accelerators"). The paper packs buffers below a fixed on-chip capacity by
// interleaving a tiered buffer-ordering heuristic with constraint-solver
// feedback and conflict-directed backtracking. This adaptation minimizes
// peak memory instead: it splits the problem into phases (connected
// components of the temporal-overlap graph), packs each phase in the
// paper's tiered order (longest lifetime, then largest size) with
// min-conflict eviction as the backtracking mechanism, and binary-searches
// each phase's capacity between its load lower bound and a first-fit
// incumbent. Evicted buffers re-enter the queue with raised priority
// (squeaky-wheel), standing in for the paper's minor/major backtracking
// levels; an occasional seeded random-walk repair breaks the cycles a purely
// deterministic min-conflict search can fall into.
class TelamallocAllocator {
 public:
  explicit TelamallocAllocator(TelamallocConfig config = TelamallocConfig{});

  [[nodiscard]] std::vector<Allocation> allocate(
      const std::vector<Allocation>& allocations) const;

 private:
  TelamallocConfig config_;
};

}  // namespace omnimalloc
