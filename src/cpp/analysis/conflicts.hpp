//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "clock.hpp"
#include "primitives/allocation.hpp"
#include "primitives/id_type.hpp"

namespace omnimalloc {

// Conflict adjacency: allocation id -> ids of conflicting allocations.
// Total: every allocation is a key, conflict-free ones map to an empty set.
using ConflictMap =
    std::unordered_map<IdType, std::unordered_set<IdType, IdTypeHash>,
                       IdTypeHash>;

// Index-based conflict adjacency: position i -> positions conflicting with i
using ConflictIndices = std::vector<std::vector<size_t>>;

// Map each allocation id to the ids of conflicting allocations (the
// happens-before conflict relation every placement packs against). A set
// `work_budget` bounds the pairwise sweep (quadratic in the worst case),
// throwing std::runtime_error instead of stalling the caller.
[[nodiscard]] ConflictMap conflicts(const std::vector<Allocation>& allocations,
                                    std::optional<uint64_t> work_budget);

// Map each allocation index to the indices of conflicting allocations
[[nodiscard]] ConflictIndices compute_conflict_indices(
    const std::vector<Allocation>& allocations);

// Total id-keyed conflict map from an index adjacency
[[nodiscard]] ConflictMap conflict_map_from_indices(
    const std::vector<Allocation>& allocations, const ConflictIndices& indices);

// Per-allocation count of conflicting allocations, aligned with
// `allocations`. Counts with multiplicity, so duplicate ids stay distinct.
// Scalar timelines count in O(N log N) without enumerating pairs; on vector
// clocks a set `work_budget` bounds the pairwise sweep (quadratic in the
// worst case), throwing std::runtime_error instead of stalling the caller.
[[nodiscard]] std::vector<int64_t> conflict_degrees(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget);

// Pairwise happens-before conflict adjacency over the pruned vector sweep;
// handles scalar and vector-clock lifetimes alike.
[[nodiscard]] CsrAdjacency build_conflict_adjacency(
    const std::vector<Allocation>& allocations);

}  // namespace omnimalloc
