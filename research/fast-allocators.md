# Fast (O(N log N)) allocators — research notes

Goal: allocators with O(N) or O(N log N) runtime that match or beat
`GreedyByAll` (best of 7 sort orders + first-fit, currently O(N²)–O(N² log N))
in peak memory. Simplicity is king.

## Why the current greedy is quadratic

- `compute_temporal_overlaps` (src/cpp/allocators/greedy_base.cpp) materializes
  every temporally-overlapping *pair* → O(N²) time/space when lifetimes overlap
  densely (the common case in NN workloads).
- `find_best_offset` scans + sorts all placed overlapping allocations per
  placement → O(N² log N) total worst case.

## Key insight for O(N log N)

First-fit needs 2D (time × offset) gap knowledge. But *skyline placement*
(place each allocation at the max of the per-time watermark over its lifetime,
then raise the watermark over that interval to offset+size) only needs a
segment tree over compressed time with range-max query + range-assign update:
O(log N) per allocation. The assign is valid because the placement offset is
by definition ≥ the watermark everywhere in the interval.

Cost vs first-fit: skyline cannot tuck allocations into holes *below* the
watermark. Hypothesis: with good orderings + compaction passes the gap is
small.

**Compaction pass (monotone, O(N log N))**: given any valid solution, re-run
skyline placement with allocations ordered by ascending current offset. By
induction each allocation's new offset ≤ old offset, so peak never increases.
Iterate a few times / until fixpoint. Cheap post-improvement for *any*
candidate.

## Idea list

| # | Idea | Complexity | Status |
|---|------|-----------|--------|
| 1 | Skyline placement, size-desc order (segment tree) | O(N log N) | todo |
| 2 | Skyline with other orders (area, duration, conflict, start) | O(N log N) | todo |
| 3 | Compaction passes (skyline re-place in ascending-offset order), iterate | O(kN log N) | todo |
| 4 | Time-sweep first-fit: chronological events, address-ordered free list (true gap reuse) | ~O(N log N) | todo |
| 5 | Time-sweep best-fit variant | ~O(N log N) | todo |
| 6 | Conflict order computed via sweep (O(N log N)) feeding skyline | O(N log N) | todo |
| 7 | Ensemble "fast by all": best of 1/2/4/5 each + compaction | O(N log N) | todo |
| 8 | Randomized restarts: perturb sort key (size × U[a,b]), keep best | O(kN log N) | todo |
| 9 | Gergov 3-approx (lit review pending — implementability?) | O(N log N)? | todo |
| 10 | Buchsbaum boxing / size-class rounding (idealloc-style) | O(N log N) | todo |
| 11 | Best-of-two-offsets skyline: try top-of-watermark vs known hole list | ? | todo |
| 12 | True first-fit in size order via offset-bucketed structures | O(N log² N)? | probably too complex |

## Benchmark setup

- Problems: 13 minimalloc CSVs (`external/minimalloc/{challenging,small,examples}`),
  synthetic sources from `omnimalloc.benchmark.sources` (random, high-contention,
  power-of-2, sequential) at N = 100 / 1000 / 5000.
- Baseline: `GreedyByAll` (C++ variants, sequential) peak.
- Lower bound: max over time of Σ active sizes (sweep) — normalizes quality.
- Quality metric: peak / lower_bound, and peak / greedy_by_all_peak.
- Runtime scaling: candidates timed at N = 1k / 10k / 50k (Python protos;
  algorithmic scaling is what matters, final impl in C++).

## Results

Metric: geomean(peak / greedy_by_all_peak) over 25 problems (13 minimalloc,
6 synthetic, small examples). <1.0 beats the quadratic baseline.

| Candidate | vs gba | Notes |
|---|---|---|
| skyline_size | 1.44 | no hole reuse → bad |
| skyline_all (6 orders) | 1.38 | still bad; compaction provably no-op on skyline |
| sweep_first_fit | 1.13 | chronological + address-ordered free list; *ties Start-order greedy exactly* (F/G/H) |
| sweep_best_fit | 1.11 | |
| sweep_two_ended | 1.13 | large→first-fit, small→best-fit |
| hybrid_size (K=4√N) | 1.11 | top-K exact first-fit + sweep-around-obstacles |
| hybrid_conflict_size | 1.09 | |
| fast_by_all (5 members + compact) | **1.02** | 3 wins / 10 ties / 12 losses |

Findings:

1. **Gap reuse is essential.** Skyline (watermark-only) placement loses ~40%.
   All good candidates use a real free list.
2. **Which greedy variant wins**: Size/ConflictSize on most minimalloc
   instances and randoms; Start on F/G/H (sweep_ff ties those exactly —
   it *is* greedy-by-start with true chronological gap reuse); Duration on D;
   Conflict on high-contention.
3. **Compaction (re-place ascending-offset via skyline) never helps sweeps** —
   provable: chronological first-fit placements are temporally blocked from
   below at placement time and stay blocked. It occasionally helps hybrids.
4. **K-sensitivity of hybrid is noisy, not monotone.** Going K=64→∞ moves
   geomean only 1.065→1.032 (size order). Ensemble diversity beats exactness:
   single exact orders at K=∞ still lose to gba (best-of-7 effect).

## Round 2 results

| Candidate | vs gba | Notes |
|---|---|---|
| noisy_restarts (8× perturbed order) | 1.02 | good anytime knob, wins on a few |
| boxed (pow2 classes + optimal channels) | 1.86 | rounding waste kills it |
| boxed_compact | 1.21 | compaction recovers a lot, still bad |
| hybrid K sweep 64→∞ | 1.065→1.032 | noisy, not monotone; ensemble > exactness |
| **fast_by_all_v2 (9 members, K=1024)** | **0.9955** | **2 wins / 22 ties / 1 loss (rand_5000 +1.7%)** |

fast_by_all_v2 members: sweep_ff, sweep_bf, sweep_two_ended, hybrid with
obstacle orders {size, conflict_size, conflict, duration, area,
area-selected+area-ordered}, best-of. Tie-break fix (keep obstacle set in
input order so stable sorts match the exact greedy variants) fixed mm_c_D.

More findings:

5. **Compaction contributes nothing to the ensemble** (helps individual weak
   members, never the final best). Dropped → no segment tree needed at all.
6. Scaling (Python protos vs C++ quadratic greedy, single variant):
   N=20k: sweep_ff 0.04 s, hybrid(K=512) 0.43 s, C++ greedy_by_size 5.9 s.
   N=50k: sweep_ff 0.11 s, hybrid 1.1 s (C++ quadratic not feasible).
7. Whole 25-problem suite: v2 in pure Python 3.5 s vs C++ greedy_by_all 3.6 s.

## Winning recipe (to be ported to C++)

1. `sweep(fit)`: sort 2N events by (time, end-before-start, size-desc);
   maintain address-ordered free list (map offset→len) with coalescing;
   place at first-fit / best-fit / two-ended (large→ff, small→bf) gap.
2. `hybrid(order, select, K)`: top-K by size (or area), exact quadratic
   first-fit on those K only (K fixed, default 1024 → O(1) asymptotically),
   then sweep the rest treating the K as fixed obstacles (forbidden offset
   bands during temporally-overlapping placements).
3. `sweep_by_all`: best of the 9 members above.

Complexity: O(N log N + N·m + K²), m = avg temporally-overlapping obstacles
per small alloc (≤ K, typically tiny). K fixed → O(N log N).

## Final implementation (shipped)

`src/cpp/allocators/sweep.{hpp,cpp}` + bindings, Python wrappers in
`allocators/sweep.py`, tests in `tests/unit/allocators/test_sweep.py`.

Allocators (all registered):

- `SweepAllocator` / `SweepBestFitAllocator` / `SweepTwoEndedAllocator` —
  chronological sweep, address-ordered coalescing free list (C++ `std::map`).
- `HybridSweepAllocator` (conflict_size order) + `BySize` / `ByDuration` /
  `ByArea` variants — exact first-fit on the top-1024 obstacles
  (`max_obstacles` configurable), sweep-around-obstacles for the rest.
  Ordering/selection stays in Python (repo pattern), placement in C++
  (`sweep_place` / `hybrid_sweep_place`).
- `SweepByAllAllocator` — best of the 7, via `allocate_parallel`.

Leave-one-out pruning dropped the plain-area and pure-conflict hybrid members
(zero ensemble contribution); 7 members mirror GreedyByAll's portfolio size.

### Final numbers (33 problems: 13 minimalloc, 6 synthetic, 8 ONNX models, 6 small)

- C++ ↔ prototype parity: **0 mismatches** across 33 × 7 runs; all valid.
- Quality: **sweep_by_all/greedy_by_all geomean 0.9967 — 2 wins, 29 ties,
  2 losses (worst +1.7%)**, i.e. matches or beats the quadratic baseline.
- Suite runtime: sweep_by_all 1.0 s sequential vs greedy_by_all 6.6 s.
- Scaling (RandomSource):

| N | sweep | hybrid | sweep_by_all | greedy_by_size (quadratic) |
|---|---|---|---|---|
| 10k | 0.01 s | 0.03 s | 0.14 s | 1.55 s |
| 50k | 0.03 s | 0.10 s | 0.57 s | ~39 s (extrap.) |
| 200k | 0.13 s | 0.39 s | 2.38 s | — |
| 1M | 0.83 s | 2.58 s | 24.4 s | ~4.3 h (extrap.) |

(sweep_by_all at 1M is dominated by pickling 8M allocations through the
process pool, not by the algorithms; single members stay near-linear.)

### Ideas tried (scoreboard)

1. Skyline/watermark seg-tree, 6 orders — 1.38–1.44, rejected (≡ TVM USMP rule)
2. Skyline ensemble + compaction — 1.38, rejected
3. Compaction passes (monotone re-place) — no ensemble contribution, dropped
4. Time-sweep first-fit — 1.11, **kept**
5. Time-sweep best-fit — 1.10, **kept**
6. Two-ended sweep (median split ff/bf) — 1.11, **kept**
7. Hybrid obstacles, 6 order/select combos — 1.05–1.17 solo, **4 kept**
8. Best-fit-among-gaps exact rule (TFLite Alg. 3) — no ensemble gain, dropped
9. Noisy-restart perturbation — 1.02, viable anytime knob, not shipped
10. Power-of-2 boxing + optimal channels (+compact) — 1.21, rejected
11. Gergov 3-approx — rejected on implementability (lit review)
12. True sub-quadratic first-fit-by-size — rejected on complexity; the
    obstacle budget makes it unnecessary (exact ≡ greedy for N ≤ 1024)

## Literature notes (from background research agent)

Theory (all ratios vs LOAD = max-over-time sum of live sizes, our `lower_bound`):

- DSA is NP-complete (Stockmeyer '76). Best implementable guarantee:
  **Gergov 1999, 3·LOAD in O(n log n)** — but it is a 2-page abstract nobody
  has ever implemented in production; research-grade reconstruction effort.
  Gergov 1996 (5-approx, via "2-allocations") is better documented.
- **Buchsbaum et al. STOC'03**: boxing/size-class recursion gives (2+ε) always
  and OPT+o(OPT) when h_max ≪ L; brutal constants (ε⁻⁶). **idealloc**
  (arXiv:2504.04874, Rust, 2025) is the first real implementation — a
  stochastic iterated box/unbox/first-fit ensemble; 0–2.6% fragmentation at
  up to 567k buffers. Notably: its bootstrap/fallback is plain
  size-desc-then-lifetime-desc first-fit ("big rocks first"), and even its
  first-fit is O(N·degree) via interference lists — *not* N log N.
- No o(OPT) additive result possible in general; gap governed by h_max/L.
- idealloc's evaluation: **no single ordering dominates across benchmarks** —
  independent confirmation of the portfolio/best-of-k approach.

Production planners:

- TFLite (Pisarchyk & Lee 2020): greedy-by-size, **best-fit-among-gaps**
  placement; hits the LB on 5/6 nets. O(N²)-ish. (Tried the best-fit-gaps
  rule in our hybrids: no ensemble gain on our suite.)
- TFLM GreedyMemoryPlanner: O(N·live-set) via purging dead records in a
  chronological sweep — industry's "quadratic but small live set" answer.
- TVM USMP greedy = pure watermark rule (no gap reuse) — **exactly equivalent
  to our skyline segment-tree placement** (which is its O(N log N) impl).
- XLA heap simulator: decreasing-size best-fit with an interval tree over
  placed chunks; O(N·overlaps) worst case.
- MiniMalloc's "canonical solutions" justify offset-ordered first-fit
  placements as a dominance class containing an optimum.

Address-ordered first-fit (our sweep):

- Brent (TOPLAS '89): AO first-fit findable in O(log) per op → chronological
  sweep is genuinely O(N log N).
- Wilson/Johnstone ('95/'98): AO-first-fit and best-fit ≈ 1% fragmentation on
  real traces; policy (address order + immediate coalescing) is what matters.
- Worst case (online): Θ(min{log h_max, log χ})-competitive (Luby–Naor–Orda
  '94) — no constant guarantee, empirically excellent. Matches our data.
