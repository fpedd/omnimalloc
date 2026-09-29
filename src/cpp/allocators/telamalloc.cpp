//
// SPDX-License-Identifier: Apache-2.0
//

#include "telamalloc.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <random>
#include <set>
#include <stdexcept>
#include <tuple>
#include <utility>

#include "analysis/clock.hpp"
#include "analysis/conflicts.hpp"
#include "common/deadline.hpp"
#include "first_fit.hpp"

namespace omnimalloc {

namespace {

// Effectively-unbounded capacity: makes a pack attempt plain first-fit.
constexpr int64_t kUnbounded = std::numeric_limits<int64_t>::max() / 4;

// Connected components of the overlap graph: the paper's "phases". Buffers
// in different components never interact, so each packs independently.
std::vector<std::vector<int>> build_phases(const CsrAdjacency& adj) {
  const int n = static_cast<int>(adj.offsets.size()) - 1;
  std::vector<std::vector<int>> phases;
  std::vector<char> visited(n, 0);
  for (int seed = 0; seed < n; ++seed) {
    if (visited[seed]) {
      continue;
    }
    std::vector<int> phase;
    std::vector<int> stack{seed};
    visited[seed] = 1;
    while (!stack.empty()) {
      int idx = stack.back();
      stack.pop_back();
      phase.push_back(idx);
      for (const int32_t other : adj.row(static_cast<size_t>(idx))) {
        if (!visited[other]) {
          visited[other] = 1;
          stack.push_back(other);
        }
      }
    }
    phases.push_back(std::move(phase));
  }
  return phases;
}

// Peak simultaneous load of `phase`: a lower bound on its achievable peak.
int64_t load_lower_bound(const std::vector<Allocation>& allocations,
                         const std::vector<int>& phase) {
  std::vector<std::pair<int64_t, int64_t>> times;
  std::vector<int64_t> weights;
  times.reserve(phase.size());
  weights.reserve(phase.size());
  for (int idx : phase) {
    times.emplace_back(allocations[idx].start(), allocations[idx].end());
    weights.push_back(allocations[idx].size());
  }
  return interval_peak(times, weights);
}

// Queue order: most-evicted first (squeaky wheel), then the paper's tiers
// (longest lifetime, largest size, or size-major when `size_major`), then
// lowest index for determinism.
using QueueKey = std::tuple<int, int64_t, int64_t, int>;

QueueKey queue_key(const Allocation& alloc, int idx, int evictions,
                   bool size_major) {
  if (size_major) {
    return {-evictions, -alloc.size(), -alloc.duration(), idx};
  }
  return {-evictions, -alloc.duration(), -alloc.size(), idx};
}

// One capacity attempt over one phase: place in queue order at the lowest
// fitting gap, evicting the cheapest blocking set on conflict and requeueing it
// at raised priority; a spent eviction budget wipes and re-packs the phase.
// Returns offsets aligned with `phase`. `offsets` and `evictions` are scratch
// indexed like `allocations`, of which only the phase's entries are touched.
std::optional<std::vector<int64_t>> pack_phase(
    const std::vector<Allocation>& allocations, const CsrAdjacency& adj,
    const std::vector<int>& phase, int64_t capacity, int max_backtracks,
    const Deadline& deadline, bool size_major, uint64_t seed,
    std::vector<int64_t>& offsets, std::vector<int>& evictions) {
  for (int idx : phase) {
    evictions[idx] = 0;
  }
  std::mt19937_64 rng(seed);

  // A restart's eviction share scales with the phase, not the budget: a
  // larger budget then buys more squeaky-wheel restarts (the diversification
  // mechanism) rather than deeper local repair within one.
  const int per_restart =
      std::min(max_backtracks, 4 * static_cast<int>(phase.size()));
  int total_backtracks = 0;

  // Scratch buffers for the placement loop, reused across queue pops.
  std::vector<std::pair<int64_t, int>> occupied;   // (offset, neighbor)
  std::vector<std::pair<int64_t, int64_t>> spans;  // (offset, end)

  while (true) {
    for (int idx : phase) {
      offsets[idx] = -1;
    }
    std::set<QueueKey> pending;
    for (int idx : phase) {
      pending.insert(
          queue_key(allocations[idx], idx, evictions[idx], size_major));
    }
    int restart_backtracks = 0;

    while (!pending.empty()) {
      if (deadline_expired(deadline)) {
        return std::nullopt;
      }
      const int idx = std::get<3>(*pending.begin());
      pending.erase(pending.begin());
      const int64_t size = allocations[idx].size();

      // Spatial intervals of the already-placed temporal neighbors. The
      // spans inherit occupied's offset order (ties differ only in end
      // order, which cannot change a gap scan's result), so one sort does.
      occupied.clear();
      for (const int32_t other : adj.row(static_cast<size_t>(idx))) {
        if (offsets[other] >= 0) {
          occupied.emplace_back(offsets[other], other);
        }
      }
      std::sort(occupied.begin(), occupied.end());
      spans.clear();
      for (const auto& [offset, other] : occupied) {
        spans.emplace_back(offset, offset + allocations[other].size());
      }

      // First-fit: lowest gap among the placed neighbors that fits. Every
      // committed offset satisfies offset + size <= capacity, so a fit below
      // a committed neighbor's offset passes the capacity check transitively.
      const int64_t cursor = first_fit_offset(size, spans);
      if (cursor + size <= capacity) {
        offsets[idx] = cursor;
        continue;
      }

      // Conflict: no gap fits below `capacity`. Score the candidate
      // placements (offset 0, flush above each blocker, flush below each
      // blocker) by the blockers each would displace; take the cheapest.
      std::vector<int64_t> candidates{0};
      for (const auto& [offset, other] : occupied) {
        const int64_t end = offset + allocations[other].size();
        if (end + size <= capacity) {
          candidates.push_back(end);
        }
        if (offset > size) {
          candidates.push_back(offset - size);
        }
      }
      int64_t best_bytes = -1;
      int best_count = 0;
      int64_t best_offset = 0;
      for (int64_t candidate : candidates) {
        int64_t bytes = 0;
        int count = 0;
        for (const auto& [offset, other] : occupied) {
          const int64_t end = offset + allocations[other].size();
          if (offset < candidate + size && end > candidate) {
            bytes += allocations[other].size();
            ++count;
          }
        }
        if (best_bytes < 0 ||
            std::tie(bytes, count, candidate) <
                std::tie(best_bytes, best_count, best_offset)) {
          best_bytes = bytes;
          best_count = count;
          best_offset = candidate;
        }
      }

      // Random walk (WalkSAT-style): occasionally take a random repair
      // instead of the cheapest one, breaking the cycles a purely
      // deterministic min-conflict search falls into.
      if (rng() % 4 == 0) {
        best_offset = candidates[rng() % candidates.size()];
      }

      for (const auto& [offset, other] : occupied) {
        const int64_t end = offset + allocations[other].size();
        if (offset < best_offset + size && end > best_offset) {
          offsets[other] = -1;
          ++evictions[other];
          pending.insert(queue_key(allocations[other], other, evictions[other],
                                   size_major));
          ++restart_backtracks;
          ++total_backtracks;
        }
      }
      offsets[idx] = best_offset;

      if (total_backtracks > max_backtracks) {
        return std::nullopt;
      }
      if (restart_backtracks > per_restart) {
        break;  // major backtrack: wipe and re-pack in squeaky-wheel order
      }
    }
    if (pending.empty()) {
      std::vector<int64_t> packed;
      packed.reserve(phase.size());
      for (int idx : phase) {
        packed.push_back(offsets[idx]);
      }
      return packed;
    }
  }
}

// Peak of phase-aligned `offsets`
int64_t phase_peak(const std::vector<Allocation>& allocations,
                   const std::vector<int>& phase,
                   const std::vector<int64_t>& offsets) {
  int64_t peak = 0;
  for (size_t k = 0; k < phase.size(); ++k) {
    peak = std::max(peak, offsets[k] + allocations[phase[k]].size());
  }
  return peak;
}

// Best-effort minimal peak for one phase: first-fit incumbent, then binary
// search on capacity down toward the load lower bound. Budget-exhausted
// attempts count as infeasible, keeping the search anytime rather than exact.
void solve_phase(const std::vector<Allocation>& allocations,
                 const CsrAdjacency& adj, const std::vector<int>& phase,
                 int64_t lower_bound, const TelamallocConfig& config,
                 const Deadline& deadline, std::vector<int64_t>& offsets,
                 std::vector<int>& evictions, std::vector<int64_t>& result) {
  const auto pack = [&](int64_t capacity, int max_backtracks,
                        const Deadline& limit, bool size_major) {
    return pack_phase(allocations, adj, phase, capacity, max_backtracks, limit,
                      size_major, config.seed, offsets, evictions);
  };
  // Unbounded capacity never conflicts, so the incumbents are plain first-fit
  // in each tiered order and cannot fail; past the deadline only the first
  // one packs. The winner's order also steers the capacity search below.
  std::vector<int64_t> best = *pack(kUnbounded, 0, std::nullopt, false);
  int64_t high = phase_peak(allocations, phase, best);
  bool size_major = false;
  if (!deadline_expired(deadline)) {
    std::vector<int64_t> by_size = *pack(kUnbounded, 0, std::nullopt, true);
    const int64_t size_peak = phase_peak(allocations, phase, by_size);
    if (size_peak < high) {
      best = std::move(by_size);
      high = size_peak;
      size_major = true;
    }
  }

  int64_t low = lower_bound;
  while (low < high && !deadline_expired(deadline)) {
    const int64_t mid = low + (high - low) / 2;
    auto attempt = pack(mid, config.max_backtracks, deadline, size_major);
    if (attempt) {
      best = std::move(*attempt);
      high = phase_peak(allocations, phase, best);
    } else {
      low = mid + 1;
    }
  }

  for (size_t k = 0; k < phase.size(); ++k) {
    result[phase[k]] = best[k];
  }
}

}  // namespace

std::vector<Allocation> telamalloc_place(
    const std::vector<Allocation>& allocations,
    const TelamallocConfig& config) {
  const Deadline deadline = make_deadline(config.timeout);
  // The event sweeps and load bounds need a linear timeline; reject vector
  // clocks here.
  require_scalar_time(allocations, "telamalloc_place");
  // Bound by kUnbounded so the unbounded-capacity pack_phase incumbents in
  // solve_phase can never fail and the cursor arithmetic cannot overflow.
  check_total_size(allocations, kUnbounded);
  const CsrAdjacency adj = build_conflict_adjacency(allocations);
  const auto phases = build_phases(adj);

  // Solve phases in descending load order: the global peak is the max over
  // phases, so the dominant phase should get the wall-clock budget first.
  std::vector<std::pair<int64_t, size_t>> order;  // (lower bound, phase)
  order.reserve(phases.size());
  for (size_t p = 0; p < phases.size(); ++p) {
    order.emplace_back(load_lower_bound(allocations, phases[p]), p);
  }
  std::sort(order.begin(), order.end(), [](const auto& a, const auto& b) {
    return a.first != b.first ? a.first > b.first : a.second < b.second;
  });

  std::vector<int64_t> offsets(allocations.size(), -1);
  std::vector<int> evictions(allocations.size(), 0);
  std::vector<int64_t> result(allocations.size(), -1);
  for (const auto& [lower_bound, p] : order) {
    solve_phase(allocations, adj, phases[p], lower_bound, config, deadline,
                offsets, evictions, result);
  }
  return apply_offsets(allocations, result);
}

}  // namespace omnimalloc
