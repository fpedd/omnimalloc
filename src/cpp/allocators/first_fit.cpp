//
// SPDX-License-Identifier: Apache-2.0
//

#include "first_fit.hpp"

#include <algorithm>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace omnimalloc {

void check_total_size(const std::vector<Allocation>& allocations,
                      int64_t limit) {
  int64_t total_size = 0;
  for (const Allocation& a : allocations) {
    if (a.size() > limit - total_size) {
      throw std::overflow_error("Total allocation size exceeds int64 range");
    }
    total_size += a.size();
  }
}

TemporalOverlaps compute_temporal_overlaps(
    const std::vector<Allocation>& allocations) {
  TemporalOverlaps overlaps;
  const OverlapIndices indices = compute_overlap_indices(allocations);
  for (size_t i = 0; i < allocations.size(); ++i) {
    for (size_t j : indices[i]) {
      overlaps[allocations[i].id()].insert(allocations[j].id());
    }
  }
  return overlaps;
}

OverlapIndices compute_overlap_indices(
    const std::vector<Allocation>& allocations) {
  std::vector<std::tuple<int64_t, bool, size_t>> events;
  events.reserve(allocations.size() * 2);
  for (size_t i = 0; i < allocations.size(); ++i) {
    events.emplace_back(allocations[i].start(), true, i);
    events.emplace_back(allocations[i].end(), false, i);
  }

  // Sort events by time; ends sort before starts at equal times, matching the
  // half-open interval semantics of Allocation::overlaps_temporally
  std::sort(events.begin(), events.end());

  OverlapIndices indices(allocations.size());
  std::vector<size_t> active;
  for (const auto& [time, is_start, idx] : events) {
    if (is_start) {
      // Current allocation overlaps with all currently active allocations
      for (size_t active_idx : active) {
        indices[idx].push_back(active_idx);
        indices[active_idx].push_back(idx);
      }
      active.push_back(idx);
    } else {
      active.erase(std::find(active.begin(), active.end(), idx));
    }
  }

  return indices;
}

void gather_spans(const std::vector<size_t>& neighbors,
                  const std::vector<std::optional<int64_t>>& offsets,
                  const std::vector<Allocation>& allocations,
                  std::vector<std::pair<int64_t, int64_t>>& spans) {
  spans.clear();
  for (size_t j : neighbors) {
    if (offsets[j].has_value()) {
      spans.emplace_back(*offsets[j], *offsets[j] + allocations[j].size());
    }
  }
  std::sort(spans.begin(), spans.end());
}

int64_t first_fit_offset(
    int64_t size, const std::vector<std::pair<int64_t, int64_t>>& spans) {
  int64_t best_offset = 0;
  for (const auto& [offset, end] : spans) {
    if (offset - best_offset >= size) {
      break;  // Found a fitting gap
    }
    best_offset = std::max(best_offset, end);
  }
  return best_offset;
}

std::vector<Allocation> first_fit_place_indexed(
    const std::vector<Allocation>& allocations, const OverlapIndices& indices) {
  // Lambda rather than the function pointer so the placement loop inlines
  // the offset scan instead of an indirect call per allocation
  return place_indexed(allocations, indices,
                       [](int64_t size, const auto& spans) {
                         return first_fit_offset(size, spans);
                       });
}

std::vector<Allocation> first_fit_place(
    const std::vector<Allocation>& allocations,
    const TemporalOverlaps& overlaps) {
  // Translate the id-keyed map into index adjacency once, so placement only
  // visits each allocation's neighbors instead of everything placed so far
  std::unordered_map<IdType, std::vector<size_t>, IdTypeHash> by_id;
  for (size_t i = 0; i < allocations.size(); ++i) {
    by_id[allocations[i].id()].push_back(i);
  }

  OverlapIndices indices(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    auto it = overlaps.find(allocations[i].id());
    if (it == overlaps.end()) {
      continue;
    }
    for (const IdType& other_id : it->second) {
      auto jt = by_id.find(other_id);
      if (jt == by_id.end()) {
        continue;
      }
      for (size_t j : jt->second) {
        if (j != i) {
          indices[i].push_back(j);
        }
      }
    }
  }

  return first_fit_place_indexed(allocations, indices);
}

FirstFitPlacer::FirstFitPlacer(std::vector<Allocation> allocations)
    : allocations_(std::move(allocations)),
      overlaps_(compute_temporal_overlaps(allocations_)),
      indices_(compute_overlap_indices(allocations_)) {
  check_total_size(allocations_);
}

std::vector<std::optional<int64_t>> FirstFitPlacer::place_offsets(
    const std::vector<size_t>& order) const {
  std::vector<std::optional<int64_t>> offsets(allocations_.size());
  std::vector<std::pair<int64_t, int64_t>> spans;
  for (size_t idx : order) {
    const Allocation& alloc = allocations_.at(idx);
    gather_spans(indices_[idx], offsets, allocations_, spans);
    offsets[idx] = first_fit_offset(alloc.size(), spans);
  }
  return offsets;
}

std::vector<Allocation> FirstFitPlacer::place(
    const std::vector<size_t>& order) const {
  const auto offsets = place_offsets(order);
  std::vector<Allocation> placed;
  placed.reserve(order.size());
  for (size_t idx : order) {
    placed.push_back(allocations_[idx].with_offset(*offsets[idx]));
  }
  return placed;
}

int64_t FirstFitPlacer::evaluate(const std::vector<size_t>& order) const {
  const auto offsets = place_offsets(order);
  int64_t peak = 0;
  for (size_t idx : order) {
    peak = std::max(peak, *offsets[idx] + allocations_[idx].size());
  }
  return peak;
}

}  // namespace omnimalloc
