//
// SPDX-License-Identifier: Apache-2.0
//

#include "sweep.hpp"

#include <algorithm>
#include <limits>
#include <map>
#include <numeric>
#include <stdexcept>
#include <utility>

#include "greedy_base.hpp"

namespace omnimalloc {

namespace {

constexpr int64_t kUnbounded = std::numeric_limits<int64_t>::max() / 4;

// Every placement offset is bounded by the live footprint, so capping the
// total size at kUnbounded guarantees the top free gap fits every request
// and no offset arithmetic overflows.
void check_total_size(const std::vector<Allocation>& allocations) {
  int64_t total = 0;
  for (const auto& alloc : allocations) {
    if (alloc.size() > kUnbounded - total) {
      throw std::overflow_error("Total allocation size exceeds sweep range");
    }
    total += alloc.size();
  }
}

// Obstacle occupying offsets [lo, hi) during times [start, end).
struct Band {
  int64_t lo;
  int64_t hi;
  int64_t start;
  int64_t end;

  auto operator<=>(const Band&) const = default;
};

// Address-ordered free list over offsets with immediate coalescing. The top
// gap is unbounded, so every request succeeds.
class FreeList {
 public:
  FreeList() { gaps_.emplace(0, kUnbounded); }

  // Lowest offset with `size` free space outside the merged, offset-sorted
  // `forbidden` bands; carves the placement out of the free list.
  int64_t take_first(
      int64_t size, const std::vector<std::pair<int64_t, int64_t>>& forbidden) {
    auto band = forbidden.begin();
    for (auto it = gaps_.begin(); it != gaps_.end(); ++it) {
      const int64_t gap_lo = it->first;
      const int64_t gap_hi = gap_lo + it->second;
      int64_t candidate = gap_lo;
      // A band that bumped the candidate past the previous gap may still
      // cover this gap's start, so back up to the first band ending above it.
      while (band != forbidden.begin() && std::prev(band)->second > candidate) {
        --band;
      }
      while (true) {
        while (band != forbidden.end() && band->second <= candidate) {
          ++band;
        }
        if (band != forbidden.end() && band->first < candidate + size) {
          candidate = band->second;
          if (candidate + size > gap_hi) {
            break;
          }
          continue;
        }
        if (candidate + size <= gap_hi) {
          carve(it, candidate, size);
          return candidate;
        }
        break;
      }
    }
    return 0;  // Unreachable: the unbounded top gap always fits.
  }

  // Smallest gap that fits (ties: lowest offset); unaware of forbidden
  // bands, so callers must fall back to take_first while bands are active.
  int64_t take_best(int64_t size) {
    auto best = gaps_.end();
    for (auto it = gaps_.begin(); it != gaps_.end(); ++it) {
      if (it->second >= size &&
          (best == gaps_.end() || it->second < best->second)) {
        best = it;
      }
    }
    const int64_t offset = best->first;
    carve(best, offset, size);
    return offset;
  }

  void release(int64_t offset, int64_t size) {
    auto next = gaps_.lower_bound(offset);
    if (next != gaps_.end() && next->first == offset + size) {
      size += next->second;
      next = gaps_.erase(next);
    }
    if (next != gaps_.begin()) {
      auto prev = std::prev(next);
      if (prev->first + prev->second == offset) {
        prev->second += size;
        return;
      }
    }
    gaps_.emplace(offset, size);
  }

 private:
  void carve(std::map<int64_t, int64_t>::iterator it, int64_t offset,
             int64_t size) {
    const int64_t gap_lo = it->first;
    const int64_t gap_hi = gap_lo + it->second;
    gaps_.erase(it);
    if (offset > gap_lo) {
      gaps_.emplace(gap_lo, offset - gap_lo);
    }
    if (gap_hi > offset + size) {
      gaps_.emplace(offset + size, gap_hi - (offset + size));
    }
  }

  std::map<int64_t, int64_t> gaps_;  // offset -> length
};

struct SweepEvent {
  int64_t time;
  bool is_start;  // false sorts first: frees precede same-time placements
  int64_t neg_size;
  size_t idx;

  auto operator<=>(const SweepEvent&) const = default;
};

int64_t median_size(const std::vector<Allocation>& allocations,
                    const std::vector<size_t>& indices) {
  std::vector<int64_t> sizes;
  sizes.reserve(indices.size());
  for (size_t idx : indices) {
    sizes.push_back(allocations[idx].size());
  }
  const auto mid = sizes.begin() + static_cast<int64_t>(sizes.size() / 2);
  std::nth_element(sizes.begin(), mid, sizes.end());
  return *mid;
}

// Sweep-place `indices` around the offset-sorted `obstacles`, writing offsets.
void sweep_indices(const std::vector<Allocation>& allocations,
                   const std::vector<size_t>& indices, SweepFit fit,
                   const std::vector<Band>& obstacles,
                   std::vector<int64_t>& offsets) {
  std::vector<SweepEvent> events;
  events.reserve(indices.size() * 2);
  for (size_t idx : indices) {
    const auto& alloc = allocations[idx];
    events.push_back({alloc.start(), true, -alloc.size(), idx});
    events.push_back({alloc.end(), false, 0, idx});
  }
  std::sort(events.begin(), events.end());

  const int64_t threshold = fit == SweepFit::kTwoEnded && !indices.empty()
                                ? median_size(allocations, indices)
                                : 0;

  FreeList free_list;
  std::vector<std::pair<int64_t, int64_t>> forbidden;
  for (const auto& event : events) {
    const auto& alloc = allocations[event.idx];
    if (!event.is_start) {
      free_list.release(offsets[event.idx], alloc.size());
      continue;
    }
    forbidden.clear();
    for (const auto& band : obstacles) {
      if (band.start < alloc.end() && alloc.start() < band.end) {
        if (!forbidden.empty() && band.lo <= forbidden.back().second) {
          forbidden.back().second = std::max(forbidden.back().second, band.hi);
        } else {
          forbidden.emplace_back(band.lo, band.hi);
        }
      }
    }
    const bool best_fit =
        (fit == SweepFit::kBest ||
         (fit == SweepFit::kTwoEnded && alloc.size() < threshold)) &&
        forbidden.empty();
    offsets[event.idx] = best_fit
                             ? free_list.take_best(alloc.size())
                             : free_list.take_first(alloc.size(), forbidden);
  }
}

std::vector<Allocation> with_offsets(const std::vector<Allocation>& allocations,
                                     const std::vector<int64_t>& offsets) {
  std::vector<Allocation> placed;
  placed.reserve(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    placed.push_back(allocations[i].with_offset(offsets[i]));
  }
  return placed;
}

}  // namespace

std::vector<Allocation> sweep_place(const std::vector<Allocation>& allocations,
                                    SweepFit fit) {
  check_total_size(allocations);
  std::vector<size_t> indices(allocations.size());
  std::iota(indices.begin(), indices.end(), size_t{0});
  std::vector<int64_t> offsets(allocations.size(), 0);
  sweep_indices(allocations, indices, fit, {}, offsets);
  return with_offsets(allocations, offsets);
}

std::vector<Allocation> hybrid_sweep_place(
    const std::vector<Allocation>& allocations, size_t num_obstacles) {
  check_total_size(allocations);
  num_obstacles = std::min(num_obstacles, allocations.size());
  const std::vector<Allocation> prefix(allocations.begin(),
                                       allocations.begin() + num_obstacles);
  auto placed = first_fit_place(prefix, compute_temporal_overlaps(prefix));

  std::vector<Band> obstacles;
  obstacles.reserve(num_obstacles);
  for (const auto& alloc : placed) {
    obstacles.push_back({alloc.offset().value(), alloc.height().value(),
                         alloc.start(), alloc.end()});
  }
  std::sort(obstacles.begin(), obstacles.end());

  std::vector<int64_t> offsets(allocations.size(), 0);
  std::vector<size_t> rest(allocations.size() - num_obstacles);
  std::iota(rest.begin(), rest.end(), num_obstacles);
  sweep_indices(allocations, rest, SweepFit::kFirst, obstacles, offsets);

  placed.reserve(allocations.size());
  for (size_t i = num_obstacles; i < allocations.size(); ++i) {
    placed.push_back(allocations[i].with_offset(offsets[i]));
  }
  return placed;
}

}  // namespace omnimalloc
