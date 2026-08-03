//
// SPDX-License-Identifier: Apache-2.0
//

#include "omni.hpp"

#include <algorithm>

#include "analysis/linearize.hpp"
#include "first_fit.hpp"

namespace omnimalloc {

std::vector<Allocation> omni_place(const std::vector<Allocation>& allocations,
                                   std::optional<uint64_t> linearize_budget) {
  if (allocations.empty()) {
    return {};
  }
  check_total_size(allocations);

  // Surrogate allocations with scalar rank times: the conflict relation is
  // identical by construction, so the placement transfers verbatim while the
  // sweep degenerates to an output-sensitive scan on a single timeline.
  const bool all_scalar =
      std::ranges::all_of(allocations, &Allocation::is_scalar_time);
  std::optional<std::vector<Allocation>> surrogates;
  if (!all_scalar) {
    surrogates = try_linearize(allocations, linearize_budget);
  }
  const std::vector<Allocation>& problem =
      surrogates.has_value() ? *surrogates : allocations;

  const CsrAdjacency adj = build_conflict_adjacency(problem);
  const PortfolioPlacement placement = place_portfolio(
      allocations, adj, surrogates.has_value() ? &*surrogates : nullptr);
  return apply_offsets(allocations, placement.offsets);
}

}  // namespace omnimalloc
