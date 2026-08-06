//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "clock.hpp"
#include "primitives/allocation.hpp"

namespace omnimalloc {

// Index-based conflict adjacency: position i -> positions conflicting with i
using ConflictIndices = std::vector<std::vector<size_t>>;

// Map each allocation index to the indices of conflicting allocations
[[nodiscard]] ConflictIndices compute_conflict_indices(
    const std::vector<Allocation>& allocations);

// Per-allocation count of conflicting allocations, aligned with `allocations`
// and counted with multiplicity. Scalar timelines count in O(N log N) without
// enumerating pairs; on vector clocks `work_budget` bounds the sweep.
[[nodiscard]] std::vector<int64_t> conflict_degrees(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget);

// Pairwise happens-before conflict adjacency over the pruned vector sweep;
// handles scalar and vector-clock lifetimes alike.
[[nodiscard]] CsrAdjacency build_conflict_adjacency(
    const std::vector<Allocation>& allocations);

// The same relation `conflicts` returns, held in CSR form at 4 bytes per
// directed edge and handed out one row at a time. Consumers that only walk the
// relation should walk it here; the id-keyed form is the memory hazard.
class ConflictGraph {
 public:
  // `max_entries` caps the neighbor entries the CSR may materialize; nullopt
  // takes kMaxAdjacencyEntries, the ceiling that keeps a near-complete graph
  // from taking the host down.
  ConflictGraph(const std::vector<Allocation>& allocations,
                std::optional<uint64_t> work_budget,
                std::optional<uint64_t> max_entries = std::nullopt);

  [[nodiscard]] size_t size() const noexcept { return adj_.offsets.size() - 1; }
  // Conflicting pairs in the relation, each counted once
  [[nodiscard]] uint64_t pair_count() const noexcept {
    return adj_.neighbors.size() / 2;
  }
  [[nodiscard]] int64_t degree(size_t index) const;
  // One row, ascending; throws std::out_of_range past the last allocation
  [[nodiscard]] std::vector<int32_t> neighbors(size_t index) const;

 private:
  void check_index(size_t index) const;

  CsrAdjacency adj_;
};

}  // namespace omnimalloc
