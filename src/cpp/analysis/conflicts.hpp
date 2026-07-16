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

// Temporal overlap adjacency: allocation id -> ids of overlapping allocations
using TemporalOverlaps =
    std::unordered_map<IdType, std::unordered_set<IdType, IdTypeHash>,
                       IdTypeHash>;

// Index-based temporal adjacency: position i -> positions overlapping i
using OverlapIndices = std::vector<std::vector<size_t>>;

// Map each allocation id to the ids of temporally overlapping allocations.
// A set `work_budget` bounds the pairwise sweep (quadratic in the worst
// case), giving up (nullopt) instead of stalling the caller.
[[nodiscard]] std::optional<TemporalOverlaps> compute_temporal_overlaps(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget);

// Map each allocation index to the indices of temporally overlapping
// allocations
[[nodiscard]] OverlapIndices compute_overlap_indices(
    const std::vector<Allocation>& allocations);

// Id-keyed overlap map from an index adjacency
[[nodiscard]] TemporalOverlaps overlaps_from_indices(
    const std::vector<Allocation>& allocations, const OverlapIndices& indices);

// Per-allocation count of temporally overlapping allocations, aligned with
// `allocations`. Counts with multiplicity, so duplicate ids stay distinct.
// A set `work_budget` bounds the pairwise sweep (quadratic in the worst
// case), giving up (nullopt) instead of stalling the caller.
[[nodiscard]] std::optional<std::vector<int64_t>> compute_conflict_degrees(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget);

// Pairwise happens-before conflict adjacency over the pruned vector sweep;
// handles scalar and vector-clock lifetimes alike.
[[nodiscard]] CsrAdjacency build_conflict_adjacency(
    const std::vector<Allocation>& allocations);

}  // namespace omnimalloc
