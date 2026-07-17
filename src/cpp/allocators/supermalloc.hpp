//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "primitives/allocation.hpp"

namespace omnimalloc {

// Per-rule toggles for the branch-and-bound pruning rules. Default-free:
// every field crosses the binding boundary explicitly (the Python face
// always enables all five; ablations set them through _cpp.try_solve_many).
struct SearchOptions {
  bool canonical;
  bool dominance;
  bool floor_inference;
  bool monotonic_floor;
  bool decompose;
};

// Search result: the placed allocations (offsets applied) and their peak,
// the placement's max height.
struct Solution {
  std::vector<Allocation> allocations;
  int64_t peak;
};

// A temporal allocation problem: immutable structural data (sections,
// overlaps, spans) shared across copies, plus mutable search state (offsets,
// floors, totals, best_height) that the hot loop updates via
// `apply_at`/`revert`.
class Partition {
 public:
  [[nodiscard]] static Partition from_allocations(
      std::vector<Allocation> allocations);

  [[nodiscard]] const std::vector<Allocation>& allocations() const noexcept {
    return data_->allocations;
  }
  // For each allocation, the largest lower index that is interchangeable with
  // it (identical span and size), or -1. Used for symmetry breaking.
  [[nodiscard]] const std::vector<int>& sym_predecessor() const noexcept {
    return data_->sym_predecessor;
  }

  // Unplaced (min_offset, idx) pairs in canonical order; maintained
  // incrementally by `apply_at`/`revert`.
  [[nodiscard]] const std::set<std::pair<int64_t, int>>& candidates()
      const noexcept {
    return candidates_;
  }

  // Per-allocation offsets; -1 indicates the allocation is unplaced.
  [[nodiscard]] const std::vector<int64_t>& offsets() const noexcept {
    return offsets_;
  }
  [[nodiscard]] int64_t best_height() const noexcept { return best_height_; }

  [[nodiscard]] bool is_allocated() const noexcept {
    return candidates_.empty();
  }
  [[nodiscard]] int64_t height() const noexcept;
  [[nodiscard]] int64_t min_height() const noexcept;
  [[nodiscard]] int64_t lower_bound() const noexcept;
  // (lower_bound, max section total) in one pass; both are loop invariants of
  // a node's candidate scan and feed `can_allocate_at`'s cheap screen.
  [[nodiscard]] std::pair<int64_t, int64_t> scan_bounds() const noexcept;

  // With `monotonic_floor`, every section the candidate does not span has its
  // floor raised to the placement offset (the canonical order places nothing
  // below it). `lower_bound` and `max_total` come from `scan_bounds` and
  // enable an O(spanned) screen; the exact scan runs only when it fails.
  [[nodiscard]] bool can_allocate_at(int idx, bool monotonic_floor,
                                     int64_t lower_bound,
                                     int64_t max_total) const noexcept;

  // Diff recorded by `apply_at` so `revert` can restore the prior state; the
  // placement's span and size are recovered from `idx` via the shared data.
  struct PlacementUndo {
    int idx;
    std::vector<std::pair<int, int64_t>> floor_changes;
    std::vector<std::pair<int, int64_t>> min_offset_changes;
  };

  // Post-`apply_at` bound check: sections untouched by the placement were
  // already validated by `can_allocate_at` with identical values, so only the
  // floors it raised (`undo.floor_changes`) can newly violate the bound.
  // `offset` is the placement offset; with `monotonic_floor` it acts as a
  // floor for every section.
  [[nodiscard]] bool placement_feasible(const PlacementUndo& undo,
                                        int64_t offset,
                                        bool monotonic_floor) const noexcept;

  // Place allocation `idx` at its `min_offset` and return the diff; `revert`
  // undoes it in O(touched log n), avoiding a per-node copy of the state. The
  // returned frame lives in a depth-indexed pool (valid until the matching
  // `revert`), so its vectors reuse their capacity across nodes.
  [[nodiscard]] const PlacementUndo& apply_at(int idx, bool floor_inference);
  void revert(const PlacementUndo& undo);
  void set_best_height(int64_t h) noexcept { best_height_ = h; }
  // Copy with `best_height` set to `bound`: a portfolio member that only
  // accepts solutions strictly below `bound`.
  [[nodiscard]] Partition with_bound(int64_t bound) const {
    Partition copy = *this;
    copy.best_height_ = bound;
    return copy;
  }

  // First-fit packing in `heuristic` order (empty keeps the input order):
  // each buffer takes the lowest gap among its already-placed overlaps. Cheap
  // incumbent for a fresh (fully unplaced) partition.
  [[nodiscard]] Solution greedy_pack(const std::string& heuristic) const;

  // Reorder allocations by `heuristic`: each character is a descending sort
  // key (one of A, C, L, O, T, U, W, Z; throws otherwise), original index
  // breaks ties. Preserves the section grid (floors, totals). The partition
  // remembers `heuristic` and reuses it to preorder `decompose` sub-parts.
  [[nodiscard]] Partition reorder(const std::string& heuristic) const;

  // Split the partition at zero-cut boundaries into independent sub-parts, or
  // nullopt when no live boundary has a zero cut. A partition reordered by a
  // non-empty heuristic preorders each sub-part on its own live section
  // totals (as minimalloc's SubSolve does), since the parent's order goes
  // stale once the problem shrinks.
  [[nodiscard]] std::optional<std::vector<Partition>> decompose() const;

 private:
  // Immutable structural data shared across copies.
  struct SharedData {
    std::vector<Allocation> allocations;
    std::vector<int64_t> alloc_sizes;
    std::vector<std::vector<int>> sections;  // section_idx -> alloc indices
    std::vector<std::vector<int>> overlaps;  // alloc_idx -> overlapping indices
    std::vector<std::pair<int, int>>
        section_spans;                 // alloc_idx -> (first, last)
    std::vector<int> sym_predecessor;  // alloc_idx -> interchangeable pred, -1
  };

  Partition(std::shared_ptr<const SharedData> data,
            std::vector<int64_t> min_offsets,
            std::vector<int64_t> section_floors,
            std::vector<int64_t> section_totals, std::vector<int64_t> offsets,
            int64_t best_height);

  // Sweep-line construction over (start, end) events.
  [[nodiscard]] static std::shared_ptr<SharedData> build_shared_data(
      std::vector<Allocation> allocations);

  // Assemble a SharedData, deriving `sym_predecessor` from `allocations`.
  [[nodiscard]] static std::shared_ptr<SharedData> make_shared_data(
      std::vector<Allocation> allocations, std::vector<int64_t> alloc_sizes,
      std::vector<std::vector<int>> sections,
      std::vector<std::vector<int>> overlaps,
      std::vector<std::pair<int, int>> section_spans);

  // Build the sub-partition for the section band [start, end), or nullopt
  // when the band contains no allocations. A non-empty `heuristic_` emits the
  // sub-part's allocations pre-sorted (see `decompose`).
  [[nodiscard]] std::optional<Partition> build_sub_partition(int start,
                                                             int end) const;

  // Sort `indices` in place by their `heuristic` keys over the section band
  // [start, end): spans clamp to the band, overlap counts include only
  // `indices` members, totals read the live section_totals_.
  void order_indices(std::vector<int>& indices, const std::string& heuristic,
                     int start, int end) const;

  // First-fit packing in `order`; body of `greedy_pack`.
  [[nodiscard]] Solution first_fit(const std::vector<int>& order) const;

  // Derive the incremental search state (candidates, tops, cuts) from
  // `offsets_`, `min_offsets_`, and the section spans.
  void init_search_state();

  std::shared_ptr<const SharedData> data_;
  std::vector<int64_t> min_offsets_;
  std::vector<int64_t> section_floors_;
  std::vector<int64_t> section_totals_;
  std::vector<int64_t> offsets_;
  int64_t best_height_;
  std::string heuristic_;  // set by `reorder`; empty means input order

  // Search state maintained incrementally by `apply_at`/`revert` so the hot
  // loop never rescans the whole problem.
  std::set<std::pair<int64_t, int>> candidates_;  // unplaced (min_offset, idx)
  std::multiset<int64_t> tops_;  // unplaced min_offset + size, for min_height
  std::vector<int64_t> cuts_;    // boundary -> unplaced allocations crossing it
  int num_zero_cuts_ = 0;

  // Undo frames are strictly stack-nested (every `apply_at` pairs with a
  // `revert`), so a depth-indexed pool reuses their vector capacity; a deque
  // keeps outstanding frames stable while deeper ones are added.
  std::deque<PlacementUndo> undo_pool_;
  size_t undo_depth_ = 0;

  // Scratch for `apply_at`'s floor inference: a section mask plus the list of
  // set entries, reset after each use so only touched sections pay.
  std::vector<char> affected_scratch_;
  std::vector<int> touched_sections_;
};

// Run `partition.greedy_pack` under each heuristic ordering across
// `num_threads` and return the best packing; ties go to the lowest heuristic
// index for determinism. Heuristics claimed after `timeout` elapse are
// skipped, except the first, so a packing always exists (nullopt disables
// the deadline). Throws std::invalid_argument when `heuristics` is empty or
// contains an unknown sort key.
[[nodiscard]] Solution greedy_pack_portfolio(
    const Partition& partition, const std::vector<std::string>& heuristics,
    std::optional<double> timeout, int num_threads);

// Run `partitions` (typically the same problem under different heuristic
// orderings) as an independent-search portfolio across `num_threads`, sharing
// one atomic best bound so a solution found by any search prunes the others.
// `max_nodes` caps each member's search independently (nullopt = unbounded).
// Returns the lowest solution found, or nullopt if none beats `best_bound` —
// a valid answer, hence the try_ prefix.
[[nodiscard]] std::optional<Solution> try_solve_many(
    const std::vector<Partition>& partitions, int64_t best_bound,
    std::optional<int64_t> max_nodes, SearchOptions options,
    std::optional<double> timeout, int num_threads);

}  // namespace omnimalloc
