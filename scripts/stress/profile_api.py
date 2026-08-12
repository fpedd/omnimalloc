#
# SPDX-License-Identifier: Apache-2.0
#
"""Profile the analysis API and the allocators across scales and workloads.

Every call runs at its shipped default budget, so a refusal is itself a
measurement; results are written per bench, so a late crash keeps the earlier ones.
"""

import argparse
import gc
import json
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from omnimalloc import allocate
from omnimalloc.allocators import BaseAllocator
from omnimalloc.analysis import (
    antichain_pressure,
    antichain_pressure_per_allocation,
    closure_pressure,
    conflict_degrees,
    conflict_graph,
    conflicts,
    try_linearize,
)
from workloads import by_name, catalog

SCALES = (100, 1_000, 10_000, 100_000, 1_000_000)

PROFILED_WORKLOADS = (
    "random",
    "high_contention",
    "skewed[dominant]",
    "tiling",
    "minimalloc[challenging]",
    "sync[sparse,t=8]",
    "sync[dense,t=8]",
    "sync[dense,t=32]",
    "concurrent_tiling[t=4]",
    "two_plus_two[noise=0.0]",
)

FAST_ALLOCATORS = (
    "omni",
    "greedy",
    "greedy_by_size",
    "greedy_by_conflict",
    "greedy_by_all",
    "best_fit",
    "naive",
)

SEARCH_ALLOCATORS = (
    "supermalloc",
    "telamalloc",
    "minimalloc",
    "simulated_annealing",
    "tabu_search",
    "hill_climb",
    "random",
)

# `TilingBase._build_tiles` rescans every tile per split, so generating one of
# these past a few thousand allocations costs more than the whole sweep.
MAX_BUILD_SCALE = 10_000
QUADRATIC_BUILDERS = frozenset({"tiling", "pinwheel", "concurrent_tiling"})

# The anytime searches spend an uninterruptible setup (the relation plus a first
# greedy pack) before their budget starts, so past this they measure setup.
MAX_SEARCH_SCALE = 10_000

# Above this a single shot is enough, and three would triple an already long run
REPEAT_LIMIT = 10_000

ANALYSIS_CALLS: dict[str, Callable] = {
    "antichain_pressure": antichain_pressure,
    "closure_pressure": closure_pressure,
    "conflict_degrees": conflict_degrees,
    "conflict_graph": lambda a: conflict_graph(a).pair_count,
    "conflicts": lambda a: len(conflicts(a)),
    "try_linearize": lambda a: try_linearize(a) is not None,
    "antichain_pressure_per_allocation": lambda a: len(
        antichain_pressure_per_allocation(a)
    ),
}


def build_capped(name: str) -> bool:
    return any(name.startswith(prefix) for prefix in QUADRATIC_BUILDERS)


def timed(call: Callable, repeats: int) -> float:
    """Best of `repeats`; the minimum is the least noisy estimator here."""
    best = float("inf")
    for _ in range(repeats):
        gc.collect()
        started = time.perf_counter()
        call()
        best = min(best, time.perf_counter() - started)
    return best


def rss_mb() -> float:
    """Current resident set; ru_maxrss is a high-water mark and cannot fall."""
    with Path("/proc/self/status").open() as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return 0.0


def bench_analysis(args: argparse.Namespace) -> list[dict]:
    rows: list[dict] = []
    index = by_name()
    for name in PROFILED_WORKLOADS:
        stalled: set[str] = set()
        for scale in args.scales:
            if build_capped(name) and scale > MAX_BUILD_SCALE:
                print(f"  analysis {name} n={scale} skipped, slow generator")
                break
            started = time.perf_counter()
            allocations = index[name].allocations(scale, 0)
            rows.append(
                {
                    "bench": "build",
                    "workload": name,
                    "n": len(allocations),
                    "seconds": time.perf_counter() - started,
                }
            )
            if len(allocations) < scale // 2:
                break  # a fixed source ran out of allocations
            for call_name, call in ANALYSIS_CALLS.items():
                rows.append(
                    _time_call(name, call_name, call, allocations, stalled, args.cutoff)
                )
            print(f"  analysis {name} n={len(allocations)} done", flush=True)
    return rows


def _time_call(
    workload: str,
    call_name: str,
    call: Callable,
    allocations: tuple,
    stalled: set[str],
    cutoff: float,
) -> dict:
    """One timing, or a recorded reason it was not taken."""
    row = {
        "bench": "analysis",
        "workload": workload,
        "call": call_name,
        "n": len(allocations),
        "dim": allocations[0].dim,
        "seconds": None,
    }
    if call_name in stalled:
        return {**row, "skipped": "an earlier scale passed the cutoff"}
    repeats = 3 if len(allocations) <= REPEAT_LIMIT else 1
    try:
        seconds = timed(lambda: call(allocations), repeats)
    except RuntimeError as e:
        stalled.add(call_name)
        return {**row, "error": f"{type(e).__name__}: {e}"[:160]}
    if seconds > cutoff:
        stalled.add(call_name)
    return {**row, "seconds": seconds, "ns_per_alloc": seconds / len(allocations) * 1e9}


def bench_allocators(args: argparse.Namespace) -> list[dict]:
    rows: list[dict] = []
    index = by_name()
    names = list(FAST_ALLOCATORS) + list(SEARCH_ALLOCATORS)
    for workload in PROFILED_WORKLOADS:
        stalled: set[str] = set()
        for scale in args.scales:
            if build_capped(workload) and scale > MAX_BUILD_SCALE:
                break
            allocations = index[workload].allocations(scale, 0)
            if len(allocations) < scale // 2:
                break
            bound = _bound_or_none(allocations)
            for name in names:
                if name in stalled:
                    continue
                if name in SEARCH_ALLOCATORS and len(allocations) > MAX_SEARCH_SCALE:
                    continue
                row = _time_allocator(workload, name, allocations, bound, args.cutoff)
                if row is None:
                    continue
                if "error" in row or row["seconds"] > args.cutoff:
                    stalled.add(name)
                rows.append(row)
            print(f"  allocators {workload} n={len(allocations)} done", flush=True)
    return rows


def _bound_or_none(allocations: tuple) -> int | None:
    try:
        return antichain_pressure(allocations)
    except RuntimeError:
        return None


def _time_allocator(
    workload: str, name: str, allocations: tuple, bound: int | None, cutoff: float
) -> dict | None:
    row = {"bench": "allocator", "workload": workload, "allocator": name}
    try:
        allocator = BaseAllocator.get(name)()
    except (ImportError, RuntimeError):
        return None
    if not allocator.supports(allocations):
        return None

    repeats = 3 if len(allocations) <= REPEAT_LIMIT else 1
    placed: tuple = ()

    def run() -> None:
        nonlocal placed
        placed = tuple(allocator.allocate(allocations))

    try:
        seconds = timed(run, repeats)
    except (RuntimeError, MemoryError, ValueError) as e:
        return {**row, "n": len(allocations), "error": f"{type(e).__name__}: {e}"[:160]}

    peak = max((a.height for a in placed if a.height is not None), default=0)
    return {
        **row,
        "n": len(allocations),
        "dim": allocations[0].dim,
        "seconds": seconds,
        "ns_per_alloc": seconds / len(allocations) * 1e9,
        "peak": peak,
        "bound": bound,
        "ratio": (peak / bound) if bound else None,
        "cutoff": cutoff,
    }


def bench_quality(args: argparse.Namespace) -> list[dict]:
    """Peak over the exact lower bound for every workload in the catalog."""
    rows = []
    for workload in catalog():
        allocations = workload.allocations(args.quality_size, 0)
        if not allocations:
            continue
        bound = _bound_or_none(allocations)
        if bound is None:
            rows.append(
                {
                    "bench": "quality",
                    "workload": workload.name,
                    "error": "the exact bound exceeds its default budget",
                }
            )
            continue
        peaks: dict[str, int] = {}
        seconds: dict[str, float] = {}
        for name in list(FAST_ALLOCATORS) + list(SEARCH_ALLOCATORS):
            placed, elapsed = _place_once(name, allocations)
            if placed is None:
                continue
            peaks[name] = max((a.height for a in placed if a.height), default=0)
            seconds[name] = elapsed
        rows.append(
            {
                "bench": "quality",
                "workload": workload.name,
                "family": workload.family,
                "dim": allocations[0].dim,
                "n": len(allocations),
                "bound": bound,
                "known_optimum": workload.known_optimum,
                "peaks": peaks,
                "seconds": seconds,
            }
        )
        print(f"  quality {workload.name} done", flush=True)
    return rows


def _place_once(name: str, allocations: tuple) -> tuple[tuple | None, float]:
    try:
        allocator = BaseAllocator.get(name)()
    except (ImportError, RuntimeError):
        return None, 0.0
    if not allocator.supports(allocations):
        return None, 0.0
    started = time.perf_counter()
    try:
        placed = tuple(allocator.allocate(allocations))
    except (RuntimeError, MemoryError, ValueError):
        return None, 0.0
    return placed, time.perf_counter() - started


def bench_concurrency(args: argparse.Namespace) -> list[dict]:
    """Check that the kernels release the GIL and stay identical under load."""
    rows = []
    index = by_name()
    for workload in ("random", "sync[dense,t=8]"):
        allocations = index[workload].allocations(args.concurrency_size, 0)
        jobs: dict[str, Callable] = {
            "omni_allocate": lambda a=allocations: [
                x.offset for x in allocate(a, "omni")
            ],
            "antichain_pressure": lambda a=allocations: antichain_pressure(a),
            "conflict_degrees": lambda a=allocations: conflict_degrees(a),
        }
        for job_name, job in jobs.items():
            try:
                expected = job()
            except RuntimeError as e:
                rows.append(
                    {
                        "bench": "concurrency",
                        "workload": workload,
                        "job": job_name,
                        "error": f"{type(e).__name__}: {e}"[:160],
                    }
                )
                continue
            rows += _scale_job(workload, job_name, job, expected, len(allocations))
            print(f"  concurrency {workload}/{job_name} done", flush=True)
    return rows


def _scale_job(
    workload: str, job_name: str, job: Callable, expected: object, count: int
) -> list[dict]:
    rows = []
    baseline = 0.0
    for workers in (1, 2, 4, 8, 16, 32):
        calls = workers * 4
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            outputs = list(pool.map(lambda _: job(), range(calls)))
        throughput = calls / (time.perf_counter() - started)
        baseline = baseline or throughput
        rows.append(
            {
                "bench": "concurrency",
                "workload": workload,
                "job": job_name,
                "workers": workers,
                "n": count,
                "calls_per_s": throughput,
                "speedup": throughput / baseline,
                "mismatches": sum(1 for out in outputs if out != expected),
            }
        )
    return rows


def bench_memory(args: argparse.Namespace) -> list[dict]:
    """Footprint of the one structure that materializes per conflicting pair."""
    rows = []
    index = by_name()
    for workload in ("high_contention", "random"):
        for scale in args.memory_sizes:
            allocations = index[workload].allocations(scale, 0)
            gc.collect()
            before = rss_mb()
            graph = conflict_graph(allocations, max_entries=None)
            pairs = graph.pair_count
            added = rss_mb() - before
            del graph
            gc.collect()
            rows.append(
                {
                    "bench": "memory",
                    "workload": workload,
                    "structure": "conflict_graph",
                    "n": len(allocations),
                    "pairs": pairs,
                    "rss_delta_mb": added,
                    "bytes_per_pair": added * 2**20 / max(pairs, 1),
                }
            )
            print(f"  memory {workload} n={scale} pairs={pairs:,}", flush=True)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scales", type=int, nargs="+", default=list(SCALES))
    parser.add_argument("--cutoff", type=float, default=20.0)
    parser.add_argument("--quality-size", type=int, default=1000)
    parser.add_argument("--concurrency-size", type=int, default=20_000)
    parser.add_argument("--memory-sizes", type=int, nargs="+", default=[10_000, 50_000])
    parser.add_argument(
        "--benches",
        nargs="+",
        default=["analysis", "allocators", "quality", "concurrency", "memory"],
    )
    parser.add_argument("--out", type=Path, default=Path("profile_results_api"))
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    results_path = args.out / "results.json"

    runners: dict[str, Callable[[argparse.Namespace], list[dict]]] = {
        "analysis": bench_analysis,
        "allocators": bench_allocators,
        "quality": bench_quality,
        "concurrency": bench_concurrency,
        "memory": bench_memory,
    }
    rows: list[dict] = []
    for bench in args.benches:
        print(f"== {bench}", flush=True)
        started = time.perf_counter()
        rows += runners[bench](args)
        # Written per bench so a later crash never discards earlier measurements
        results_path.write_text(json.dumps(rows, indent=1, default=str))
        print(f"== {bench} took {time.perf_counter() - started:.0f}s", flush=True)

    print(f"{len(rows)} rows, wrote {results_path}")


if __name__ == "__main__":
    main()
