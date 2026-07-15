//
// SPDX-License-Identifier: Apache-2.0
//

#include "closure.hpp"

#include <algorithm>
#include <cstdint>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clock_rows.hpp"
#include "common/parallel.hpp"

// Join-closure enumeration of the cut lattice. Cuts live in a flat row
// arena behind a hash set of row indices: a candidate join is written
// provisionally at the arena tail and rolled back when already present, so
// the arena always holds exactly the distinct cuts, ready for parallel
// scoring. Identical (start, end) lifetimes are pre-merged; each cut then
// scores the summed weight of groups born at or before it and not yet dead.

namespace omnimalloc {

namespace {

// The join-closure of the group birth clocks as a flat num_cuts x d arena,
// or nullopt once it exceeds `closure_cap`.
std::optional<std::vector<int64_t>> build_cut_arena(
    const LifetimeGroups& groups, size_t d, size_t closure_cap) {
  std::vector<int64_t> arena;
  const auto row = [&](size_t idx) { return arena.data() + idx * d; };
  const auto hash_row = [&](size_t idx) {
    uint64_t hash = 0xcbf29ce484222325ULL;  // FNV-1a over the components
    const int64_t* r = row(idx);
    for (size_t t = 0; t < d; ++t) {
      hash = (hash ^ static_cast<uint64_t>(r[t])) * 0x100000001b3ULL;
    }
    return static_cast<size_t>(hash);
  };
  const auto eq_row = [&](size_t a, size_t b) {
    return std::equal(row(a), row(a) + d, row(b));
  };
  std::unordered_set<size_t, decltype(hash_row), decltype(eq_row)> cuts(
      0, hash_row, eq_row);
  // Keeps the provisional row at the arena tail if new, rolls it back if
  // already present
  const auto insert_tail = [&]() {
    const auto [it, inserted] = cuts.insert(arena.size() / d - 1);
    if (!inserted) {
      arena.resize(arena.size() - d);
    }
    return inserted;
  };

  std::vector<size_t> births;
  for (size_t i = 0; i < groups.count(); ++i) {
    arena.insert(arena.end(), groups.starts[i].begin(), groups.starts[i].end());
    if (insert_tail()) {
      if (cuts.size() > closure_cap) {
        return std::nullopt;
      }
      births.push_back(arena.size() / d - 1);
    }
  }

  std::vector<size_t> frontier(births);
  while (!frontier.empty()) {
    const size_t cut = frontier.back();
    frontier.pop_back();
    for (const size_t birth : births) {
      const size_t tail = arena.size();
      arena.resize(tail + d);
      for (size_t t = 0; t < d; ++t) {
        arena[tail + t] = std::max(row(cut)[t], row(birth)[t]);
      }
      if (insert_tail()) {
        if (cuts.size() > closure_cap) {
          return std::nullopt;
        }
        frontier.push_back(arena.size() / d - 1);
      }
    }
  }
  return arena;
}

// Score every cut: groups born at or before it and not yet dead. Each
// allocation is live at its own birth cut, so the max covers singletons.
std::vector<int64_t> live_weights(const std::vector<int64_t>& arena,
                                  const LifetimeGroups& groups, size_t d) {
  const size_t num_cuts = arena.size() / d;
  std::vector<int64_t> live(num_cuts);
  for_each_row_block(num_cuts, parallel_threads(num_cuts), [&](size_t c) {
    const int64_t* cut = arena.data() + c * d;
    int64_t weight = 0;
    for (size_t i = 0; i < groups.count(); ++i) {
      const bool born = dominates(groups.starts[i].data(), cut, d);
      const bool dead = dominates(groups.ends[i].data(), cut, d);
      weight += (born && !dead) ? groups.weights[i] : 0;
    }
    live[c] = weight;
  });
  return live;
}

std::vector<std::pair<int64_t, int64_t>> scalar_times(
    const LifetimeGroups& groups) {
  std::vector<std::pair<int64_t, int64_t>> times(groups.count());
  for (size_t i = 0; i < groups.count(); ++i) {
    times[i] = {groups.starts[i][0], groups.ends[i][0]};
  }
  return times;
}

}  // namespace

std::optional<int64_t> closure_pressure(
    const std::vector<Allocation>& allocations, size_t closure_cap) {
  if (allocations.empty()) {
    return 0;
  }
  const size_t d = checked_dim(allocations);
  const LifetimeGroups groups = group_lifetimes(allocations);

  // Scalar cuts are plain time points; the sweep is the same quantity and
  // needs no cap
  if (d == 1) {
    return interval_peak(scalar_times(groups), groups.weights);
  }

  const auto arena = build_cut_arena(groups, d, closure_cap);
  if (!arena.has_value()) {
    return std::nullopt;
  }
  const std::vector<int64_t> live = live_weights(*arena, groups, d);
  return *std::ranges::max_element(live);
}

std::optional<std::vector<int64_t>> per_allocation_closure_pressure(
    const std::vector<Allocation>& allocations, size_t closure_cap) {
  const size_t n = allocations.size();
  if (n == 0) {
    return std::vector<int64_t>{};
  }
  const size_t d = checked_dim(allocations);
  const LifetimeGroups groups = group_lifetimes(allocations);
  const size_t g = groups.count();

  std::vector<int64_t> per_group;
  if (d == 1) {
    per_group = interval_peaks(scalar_times(groups), groups.weights);
  } else {
    const auto arena = build_cut_arena(groups, d, closure_cap);
    if (!arena.has_value()) {
      return std::nullopt;
    }
    const std::vector<int64_t> live = live_weights(*arena, groups, d);
    // Peak per group: the heaviest cut where it is live; its own birth cut
    // always qualifies, so every entry is at least the group weight.
    const size_t num_cuts = arena->size() / d;
    per_group.resize(g);
    for_each_row_block(g, parallel_threads(g), [&](size_t i) {
      const int64_t* start = groups.starts[i].data();
      const int64_t* end = groups.ends[i].data();
      int64_t best = 0;
      for (size_t c = 0; c < num_cuts; ++c) {
        const int64_t* cut = arena->data() + c * d;
        if (dominates(start, cut, d) && !dominates(end, cut, d)) {
          best = std::max(best, live[c]);
        }
      }
      per_group[i] = best;
    });
  }

  std::vector<int64_t> peaks(n);
  for (size_t i = 0; i < n; ++i) {
    peaks[i] = per_group[static_cast<size_t>(groups.group[i])];
  }
  return peaks;
}

}  // namespace omnimalloc
