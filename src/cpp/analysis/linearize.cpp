//
// SPDX-License-Identifier: Apache-2.0
//

#include "linearize.hpp"

#include <algorithm>
#include <atomic>
#include <numeric>
#include <random>

#include "clock.hpp"
#include "common/parallel.hpp"

// Fishburn's interval-order test without materializing predecessor sets: the
// order is an interval order iff the family pred(j) = {x : end(x) <= start(j)}
// forms an inclusion chain, and the surrogate times are ranks in that chain.

namespace omnimalloc {

namespace {

// Weighted |{ends e : e <= start componentwise}|. The rows ascend on
// component 0, so only the prefix with e[0] <= start[0] can qualify.
int64_t dominated_weight(const DedupedRows& ends,
                         const int64_t* start) noexcept {
  const size_t d = ends.dim;
  const size_t lo = ends.prefix_leq(start[0]);
  int64_t count = 0;
  for (size_t j = 0; j < lo; ++j) {
    count += dominates(ends.row(j), start, d) ? ends.weights[j] : 0;
  }
  return count;
}

// A predecessor of `yes` that is not a predecessor of `no`, probed among the
// `window` end rows just below the component-0 boundary: rows ascend there, so
// near-boundary ends are the likeliest to split two predecessor sets apart.
bool split_witness(const DedupedRows& ends, const int64_t* yes,
                   const int64_t* no, size_t window) noexcept {
  const size_t d = ends.dim;
  const size_t lo = ends.prefix_leq(yes[0]);
  const size_t begin = lo > window ? lo - window : 0;
  for (size_t j = lo; j-- > begin;) {
    if (dominates(ends.row(j), yes, d) && !dominates(ends.row(j), no, d)) {
      return true;
    }
  }
  return false;
}

// 2+2 witnesses: two incomparable predecessor sets, so non-linearizable
// instances bail before the full O(k * m * d) chain test. A random global pass
// catches coarse concurrency, an adjacent-start-pair pass the local kind.
bool find_incomparability_witness(const DedupedRows& starts,
                                  const DedupedRows& ends) {
  if (starts.count() < 2 || ends.count() < 2) {
    return false;
  }
  std::mt19937 rng(0x9e3779b9);
  std::uniform_int_distribution<size_t> pick_start(0, starts.count() - 1);
  std::uniform_int_distribution<size_t> pick_end(0, ends.count() - 1);
  constexpr int kSamples = 256;
  const size_t d = starts.dim;
  for (int i = 0; i < kSamples; ++i) {
    const int64_t* s1 = starts.row(pick_start(rng));
    const int64_t* s2 = starts.row(pick_start(rng));
    const int64_t* e1 = ends.row(pick_end(rng));
    const int64_t* e2 = ends.row(pick_end(rng));
    if (dominates(e1, s1, d) && !dominates(e1, s2, d) && dominates(e2, s2, d) &&
        !dominates(e2, s1, d)) {
      return true;
    }
  }
  constexpr size_t kWindow = 16;
  for (size_t p = 0; p + 1 < starts.count(); ++p) {
    const int64_t* s1 = starts.row(p);
    const int64_t* s2 = starts.row(p + 1);
    if (split_witness(ends, s1, s2, kWindow) &&
        split_witness(ends, s2, s1, kWindow)) {
      return true;
    }
  }
  return false;
}

}  // namespace

std::optional<std::vector<std::pair<int64_t, int64_t>>> linearize_times(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget, bool* undecided) {
  const size_t n = allocations.size();
  std::vector<std::pair<int64_t, int64_t>> times(n);
  if (n == 0) {
    return times;
  }

  ClockSpans spans = gather_clock_spans(allocations);
  // The reduction scans at least 2 * n * d clock components, so it only runs
  // when the budget covers that pass; in particular `work_budget=0` (the
  // contract for disabling linearization) refuses vector input untouched.
  std::vector<int64_t> backing;
  if (!work_budget ||
      2 * static_cast<uint64_t>(n) * spans.dim <= *work_budget) {
    backing = reduce_columns(spans);
  }
  const size_t d = spans.dim;
  if (d == 1) {
    for (size_t i = 0; i < n; ++i) {
      times[i] = {spans.starts[i][0], spans.ends[i][0]};
    }
    return times;
  }

  const DedupedRows starts = dedupe_rows(spans.starts, d);
  const DedupedRows ends = dedupe_rows(spans.ends, d);
  const size_t k = starts.count();
  const size_t m = ends.count();

  // Dominance counting costs O(k * m * d) before pruning; under a set
  // budget, give up undecided instead of stalling the caller (and before
  // spending the witness scan on an instance already refused).
  if (work_budget && static_cast<uint64_t>(k) * m * d > *work_budget) {
    if (undecided != nullptr) {
      *undecided = true;
    }
    return std::nullopt;
  }
  if (find_incomparability_witness(starts, ends)) {
    return std::nullopt;
  }

  // Predecessor-set size per distinct start, with multiplicity
  std::vector<int64_t> counts(k);
  for_each_row_block(k, parallel_threads(k), [&](size_t si) {
    counts[si] = dominated_weight(ends, starts.row(si));
  });

  // Chain test on count-sorted starts: adjacent pairs must satisfy
  // pred(a) ⊆ pred(b), checked as count(meet(a, b)) == count(a). Inclusion is
  // transitive, so adjacent pairs cover the whole family.
  std::vector<int32_t> by_count(k);
  std::iota(by_count.begin(), by_count.end(), 0);
  std::stable_sort(by_count.begin(), by_count.end(), [&](int32_t a, int32_t b) {
    return counts[static_cast<size_t>(a)] < counts[static_cast<size_t>(b)];
  });
  // Componentwise meets of count-adjacent start rows, precomputed so the
  // parallel chain test allocates nothing per pair
  std::vector<int64_t> meets((k - 1) * d);
  for (size_t pos = 0; pos + 1 < k; ++pos) {
    const int64_t* a = starts.row(static_cast<size_t>(by_count[pos]));
    const int64_t* b = starts.row(static_cast<size_t>(by_count[pos + 1]));
    for (size_t t = 0; t < d; ++t) {
      meets[pos * d + t] = std::min(a[t], b[t]);
    }
  }
  std::atomic<bool> is_chain{true};
  for_each_row_block(k - 1, parallel_threads(k), [&](size_t pos) {
    if (!is_chain.load(std::memory_order_relaxed)) {
      return;
    }
    if (dominated_weight(ends, meets.data() + pos * d) !=
        counts[static_cast<size_t>(by_count[pos])]) {
      is_chain.store(false, std::memory_order_relaxed);
    }
  });
  if (!is_chain.load(std::memory_order_relaxed)) {
    return std::nullopt;
  }

  // Ranks: distinct counts ascending. On a chain, equal counts mean equal
  // predecessor sets, so they share a rank and any member represents it.
  std::vector<int64_t> unique_counts;
  std::vector<int32_t> representative;
  for (size_t pos = 0; pos < k; ++pos) {
    const auto si = static_cast<size_t>(by_count[pos]);
    if (unique_counts.empty() || counts[si] != unique_counts.back()) {
      unique_counts.push_back(counts[si]);
      representative.push_back(by_count[pos]);
    }
  }
  std::vector<int64_t> start_rank(k);
  for (size_t si = 0; si < k; ++si) {
    start_rank[si] = std::lower_bound(unique_counts.begin(),
                                      unique_counts.end(), counts[si]) -
                     unique_counts.begin();
  }

  // End rank: smallest rank whose representative start dominates the end,
  // binary-searchable because nested predecessor sets make membership monotone
  // in rank. Strictness end' > start' follows from validation.
  const auto num_ranks = static_cast<int64_t>(unique_counts.size());
  std::vector<int64_t> end_rank(m);
  for_each_row_block(m, parallel_threads(m), [&](size_t ej) {
    const int64_t* e = ends.row(ej);
    int64_t lo = 0;
    int64_t hi = num_ranks;
    while (lo < hi) {
      const int64_t mid = lo + (hi - lo) / 2;
      const int64_t* s = starts.row(
          static_cast<size_t>(representative[static_cast<size_t>(mid)]));
      if (dominates(e, s, d)) {
        hi = mid;
      } else {
        lo = mid + 1;
      }
    }
    end_rank[ej] = lo;
  });

  for (size_t i = 0; i < n; ++i) {
    times[i] = {start_rank[static_cast<size_t>(starts.group[i])],
                end_rank[static_cast<size_t>(ends.group[i])]};
  }
  return times;
}

std::optional<std::vector<Allocation>> try_linearize(
    const std::vector<Allocation>& allocations,
    std::optional<uint64_t> work_budget, bool* undecided) {
  const auto times = linearize_times(allocations, work_budget, undecided);
  if (!times.has_value()) {
    return std::nullopt;
  }
  std::vector<Allocation> linearized;
  linearized.reserve(allocations.size());
  for (size_t i = 0; i < allocations.size(); ++i) {
    const Allocation& a = allocations[i];
    linearized.emplace_back(a.id(), a.size(), (*times)[i].first,
                            (*times)[i].second, a.offset(), a.kind());
  }
  return linearized;
}

}  // namespace omnimalloc
