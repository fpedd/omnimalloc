//
// SPDX-License-Identifier: Apache-2.0
//

#include "conflicts.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <string_view>

#include "common/parallel.hpp"

namespace omnimalloc {

namespace {

// The pruned pairwise sweep lives in analysis/clock.hpp (ConflictSweep), shared
// with the per-allocation pressure kernels. Scalar input, including clocks that
// collapse to one column, degenerates to an output-sensitive sweep line.
ConflictSweep build_conflict_sweep(const std::vector<Allocation>& allocations) {
  ClockSpans spans = gather_clock_spans(allocations);
  // Safe to drop `backing` here only because ConflictSweep copies its inputs
  const std::vector<int64_t> backing = reduce_columns(spans);
  return {spans.starts, spans.ends, spans.dim};
}

// Refuse a sweep that would outrun `work_budget`. `what` completes
// "pass None to always ...", naming what the caller was about to do.
void check_sweep_budget(const ConflictSweep& sweep,
                        std::optional<uint64_t> work_budget,
                        std::string_view what) {
  if (work_budget && sweep.sweep_work() > *work_budget) {
    throw std::runtime_error(
        "Conflict sweep work exceeds work_budget; pass None to always " +
        std::string(what));
  }
}

// Pairwise happens-before adjacency unpacked from CSR form
ConflictIndices indices_from_adjacency(size_t n, const CsrAdjacency& adj) {
  ConflictIndices indices(n);
  for (size_t i = 0; i < n; ++i) {
    indices[i].assign(adj.neighbors.begin() + adj.offsets[i],
                      adj.neighbors.begin() + adj.offsets[i + 1]);
  }
  return indices;
}

}  // namespace

CsrAdjacency build_conflict_adjacency(
    const std::vector<Allocation>& allocations) {
  const ConflictSweep sweep = build_conflict_sweep(allocations);
  return sweep.adjacency(parallel_threads(sweep.count()));
}

ConflictIndices compute_conflict_indices(
    const std::vector<Allocation>& allocations) {
  return indices_from_adjacency(allocations.size(),
                                build_conflict_adjacency(allocations));
}

ConflictGraph::ConflictGraph(const std::vector<Allocation>& allocations,
                             std::optional<uint64_t> work_budget,
                             std::optional<uint64_t> max_entries) {
  // Bounds the sweep only; the CSR the sweep fills is guarded by its own
  // entry ceiling in ConflictSweep::adjacency
  const ConflictSweep sweep = build_conflict_sweep(allocations);
  check_sweep_budget(sweep, work_budget, "compute the relation");
  const unsigned threads = parallel_threads(sweep.count());
  adj_ = sweep.adjacency(threads, max_entries.value_or(kMaxAdjacencyEntries));
  // The parallel fill leaves rows unordered; ordering them once here keeps
  // repeated reads of the same row off the sort.
  for_each_row_block(size(), threads, [&](size_t row) {
    std::ranges::sort(adj_.neighbors.begin() + adj_.offsets[row],
                      adj_.neighbors.begin() + adj_.offsets[row + 1]);
  });
}

void ConflictGraph::check_index(size_t index) const {
  if (index >= size()) {
    throw std::out_of_range("index " + std::to_string(index) +
                            " out of range for " + std::to_string(size()) +
                            " allocations");
  }
}

int64_t ConflictGraph::degree(size_t index) const {
  check_index(index);
  return adj_.offsets[index + 1] - adj_.offsets[index];
}

std::vector<int32_t> ConflictGraph::neighbors(size_t index) const {
  check_index(index);
  return {adj_.neighbors.begin() + adj_.offsets[index],
          adj_.neighbors.begin() + adj_.offsets[index + 1]};
}

std::vector<int64_t> conflict_degrees(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget) {
  // Scalar timelines count in O(N log N) binary searches inside the sweep,
  // so the budget guards only the pair-enumerating vector path.
  const ConflictSweep sweep = build_conflict_sweep(allocations);
  if (sweep.dim() > 1) {
    check_sweep_budget(sweep, work_budget, "count");
  }
  return sweep.degrees(parallel_threads(sweep.count()));
}

}  // namespace omnimalloc
