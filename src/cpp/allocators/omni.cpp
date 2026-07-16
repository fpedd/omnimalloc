//
// SPDX-License-Identifier: Apache-2.0
//

#include "omni.hpp"

#include <algorithm>
#include <optional>

#include "analysis/linearize.hpp"
#include "first_fit.hpp"

namespace omnimalloc {

std::vector<Allocation> OmniAllocator::allocate(
    const std::vector<Allocation>& allocations) const {
  if (allocations.empty()) {
    return {};
  }
  check_total_size(allocations);

  // Surrogate allocations with scalar rank times: the conflict relation is
  // identical by construction, so the placement transfers verbatim, while
  // the conflict sweep degenerates to a plain output-sensitive scan on the
  // single timeline. The budget keeps the linearize attempt from dominating
  // the placement it is meant to speed up.
  const bool all_scalar =
      std::ranges::all_of(allocations, &Allocation::is_scalar_time);
  std::optional<std::vector<Allocation>> surrogates;
  if (!all_scalar) {
    surrogates = try_linearize(allocations, linearize_budget_);
  }
  const std::vector<Allocation>& problem =
      surrogates.has_value() ? *surrogates : allocations;

  const CsrAdjacency adj = build_conflict_adjacency(problem);
  const PortfolioPlacement placement = place_portfolio(problem, adj);

  std::vector<Allocation> placed;
  placed.reserve(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    placed.push_back(allocations[i].with_offset(placement.offsets[i]));
  }
  return placed;
}

}  // namespace omnimalloc

namespace std {

size_t hash<omnimalloc::OmniAllocator>::operator()(
    const omnimalloc::OmniAllocator& allocator) const noexcept {
  return hash<std::optional<uint64_t>>{}(allocator.linearize_budget());
}

}  // namespace std
