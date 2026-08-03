//
// SPDX-License-Identifier: Apache-2.0
//

#include "collision.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <span>
#include <stdexcept>
#include <vector>

#include "clock.hpp"
#include "common/parallel.hpp"

// A placement is sound iff no pair both conflicts in time and overlaps in
// address, so either axis alone bounds the search, and soundness makes the two
// trade off. The cheaper sweep runs; the other axis becomes its predicate.

namespace omnimalloc {

namespace {

// The address axis as a one-dimensional clock: [offset, offset + size) stands
// in for a lifetime, so overlapping addresses are exactly conflicting rows and
// the shared sweep prunes the axis unchanged. It copies, so the rows are local.
ConflictSweep address_sweep(const std::vector<Allocation>& allocations) {
  const size_t n = allocations.size();
  std::vector<int64_t> offsets(n);
  std::vector<int64_t> tops(n);
  std::vector<std::span<const int64_t>> offset_rows;
  std::vector<std::span<const int64_t>> top_rows;
  offset_rows.reserve(n);
  top_rows.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    offsets[i] = *allocations[i].offset();
    tops[i] = offsets[i] + allocations[i].size();
  }
  for (size_t i = 0; i < n; ++i) {
    offset_rows.emplace_back(&offsets[i], 1);
    top_rows.emplace_back(&tops[i], 1);
  }
  return {offset_rows, top_rows, 1};
}

}  // namespace

std::optional<std::pair<size_t, size_t>> find_collision(
    const std::vector<Allocation>& allocations) {
  const size_t n = allocations.size();
  if (n < 2) {
    return std::nullopt;
  }
  for (const Allocation& alloc : allocations) {
    if (!alloc.offset().has_value()) {
      throw std::invalid_argument(
          "Collision search requires placed allocations");
    }
  }
  ClockSpans spans = gather_clock_spans(allocations);
  const std::vector<int64_t> backing = reduce_columns(spans);

  // Packed as low * n + high, which orders pairs lexicographically, so taking
  // the minimum makes the reported pair independent of how the sweep was
  // scheduled. n fits int32 indices, so the product stays inside int64.
  const auto no_collision = std::numeric_limits<uint64_t>::max();
  std::atomic<uint64_t> best{no_collision};
  const auto report = [&](size_t i, size_t j) {
    const auto [low, high] = std::minmax(i, j);
    atomic_fetch_min(best, static_cast<uint64_t>(low) * n + high);
  };

  const ConflictSweep space = address_sweep(allocations);
  const ConflictSweep time(spans.starts, spans.ends, spans.dim);
  // sweep_work() counts component comparisons; a scanned address pair costs
  // one dominance test, so scale to the same unit before choosing
  if (space.sweep_work() * spans.dim <= time.sweep_work()) {
    space.for_each_pair(parallel_threads(n), [&](size_t i, size_t j) {
      if (!happens_before(spans.ends[i], spans.starts[j]) &&
          !happens_before(spans.ends[j], spans.starts[i])) {
        report(i, j);
      }
    });
  } else {
    time.for_each_pair(parallel_threads(n), [&](size_t i, size_t j) {
      if (allocations[i].overlaps_spatially(allocations[j])) {
        report(i, j);
      }
    });
  }

  const uint64_t found = best.load(std::memory_order_relaxed);
  if (found == no_collision) {
    return std::nullopt;
  }
  return std::pair{static_cast<size_t>(found / n),
                   static_cast<size_t>(found % n)};
}

}  // namespace omnimalloc
