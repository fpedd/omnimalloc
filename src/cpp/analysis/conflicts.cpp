//
// SPDX-License-Identifier: Apache-2.0
//

#include "conflicts.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "common/parallel.hpp"

namespace omnimalloc {

CsrAdjacency build_conflict_adjacency(
    const std::vector<Allocation>& allocations) {
  const ConflictSweep sweep(allocations);
  return sweep.adjacency(parallel_threads(sweep.count()));
}

ConflictGraph::ConflictGraph(const std::vector<Allocation>& allocations,
                             std::optional<uint64_t> work_budget,
                             std::optional<uint64_t> max_entries) {
  // Bounds the sweep only; the CSR the sweep fills is guarded by its own
  // entry ceiling in ConflictSweep::adjacency
  const ConflictSweep sweep(allocations);
  sweep.check_budget(work_budget, "compute the relation");
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

size_t ConflictGraph::degree(size_t index) const {
  check_index(index);
  return adj_.row(index).size();
}

std::vector<int32_t> ConflictGraph::neighbors(size_t index) const {
  check_index(index);
  const auto row = adj_.row(index);
  return {row.begin(), row.end()};
}

std::vector<int64_t> conflict_degrees(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget) {
  // Scalar timelines count in O(N log N) binary searches inside the sweep,
  // so the budget guards only the pair-enumerating vector path.
  const ConflictSweep sweep(allocations);
  if (sweep.dim() > 1) {
    sweep.check_budget(work_budget, "count");
  }
  return sweep.degrees(parallel_threads(sweep.count()));
}

}  // namespace omnimalloc
