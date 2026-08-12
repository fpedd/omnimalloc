#
# SPDX-License-Identifier: Apache-2.0
#
"""Sweep every workload through the analysis API and every applicable allocator.

Cells run on a thread pool, so the kernels are exercised concurrently and a
race in a GIL-releasing kernel surfaces as a mismatch rather than staying quiet.
"""

import argparse
import json
import random
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from invariants import (
    check_conflicts,
    check_lane_invariance,
    check_linearize,
    check_order_invariance,
    check_pickle,
    check_placement,
    check_pressure,
    peak_of,
)
from omnimalloc import allocate, validate_allocation
from omnimalloc.allocators import BaseAllocator
from omnimalloc.analysis import antichain_pressure
from omnimalloc.primitives import Allocation, IdType, Memory, Pool, System
from workloads import Workload, catalog

# The search allocators get a short budget: this sweep checks contracts, and
# their default seconds-scale budgets would dominate its runtime.
FAST_KWARGS: dict[str, dict[str, float | int]] = {
    "genetic": {"population_size": 12, "generations": 4, "timeout": 1.0},
    "simulated_annealing": {"max_iterations": 500, "timeout": 1.0},
    "tabu_search": {"max_iterations": 200, "timeout": 1.0},
    "hill_climb": {"iterations": 200, "timeout": 1.0},
    "supermalloc": {"timeout": 1.0},
    "telamalloc": {"timeout": 1.0},
}

# Brute-force references are quadratic, so they run only on small instances
BRUTE_LIMIT = 200
CLIQUE_LIMIT = 26

# Wrapper around a separately installed package, absent by default
OPTIONAL = frozenset({"minimalloc"})


def build_allocator(name: str) -> BaseAllocator | None:
    """Instantiate with the sweep's short budget, or plainly, or not at all."""
    allocator_type = BaseAllocator.get(name)
    try:
        return allocator_type(**FAST_KWARGS.get(name, {}))
    except (TypeError, ImportError, RuntimeError):
        try:
            return allocator_type()
        except Exception:  # noqa: BLE001
            return None


def fresh(name: str) -> BaseAllocator:
    """A new instance of an allocator already known to construct.

    Search allocators carry per-run state, so every call gets its own.
    """
    allocator = build_allocator(name)
    assert allocator is not None
    return allocator


def pin_subset(
    placed: tuple[Allocation, ...], fraction: float, rng: random.Random
) -> tuple[tuple[Allocation, ...], dict[IdType, int]]:
    """Re-pose a known-good placement with a random subset pinned."""
    pinned = tuple(
        alloc if rng.random() < fraction else alloc.with_offset(None)
        for alloc in placed
    )
    return pinned, {a.id: a.offset for a in pinned if a.offset is not None}


def check_hierarchy(
    allocations: tuple[Allocation, ...], allocator: BaseAllocator
) -> list[str]:
    """The System, Memory and Pool paths must agree with the flat one."""
    fails = []
    flat = allocate(allocations, allocator)
    from_pool = allocate(Pool(id="p0", allocations=allocations), allocator)
    if [a.offset for a in from_pool.allocations] != [a.offset for a in flat]:
        fails.append("the Pool path disagrees with the flat path")
    if [a.id for a in from_pool.allocations] != [a.id for a in allocations]:
        fails.append("Pool.allocate did not preserve the input order")

    half = len(allocations) // 2 or len(allocations)
    renamed = tuple(
        Allocation(id=f"b_{a.id}", size=a.size, start=a.start, end=a.end, kind=a.kind)
        for a in allocations[half:]
    )
    memory = Memory(
        id="m0",
        pools=(
            Pool(id="a", allocations=allocations[:half]),
            Pool(id="b", allocations=renamed),
        ),
    )
    placed = allocate(System(id="s0", memories=(memory,)), allocator)
    if not placed.is_allocated:
        fails.append("System.allocate left something unplaced")
    try:
        validate_allocation(placed)
    except ValueError as e:
        fails.append(f"the System placement failed validation, {e}")

    pools = placed.memories[0].pools
    based = [(pool.offset, pool) for pool in pools if pool.offset is not None]
    if len(based) != len(pools):
        fails.append("Memory.allocate left a pool without a base")
    elif placed.memories[0].extent < max(base + pool.size for base, pool in based):
        fails.append("Memory.extent sits below the top of its pools")
    return fails


def check_allocator(
    allocations: tuple[Allocation, ...],
    name: str,
    rng: random.Random,
    deep: bool,
) -> tuple[list[str], int | None, float | None]:
    """Run one allocator and check its result, its determinism, and its pins."""
    allocator = build_allocator(name)
    if allocator is None or not allocator.supports(allocations):
        return [], None, None
    try:
        started = time.perf_counter()
        placed = tuple(allocator.allocate(allocations))
        seconds = time.perf_counter() - started
    except Exception as e:  # noqa: BLE001
        if name in OPTIONAL and isinstance(e, ImportError | RuntimeError):
            return [], None, None
        return [f"{name}: raised {type(e).__name__}, {e}"], None, None

    fails = check_placement(allocations, placed, name, {})
    repeat = tuple(fresh(name).allocate(allocations))
    if [a.offset for a in repeat] != [a.offset for a in placed]:
        fails.append(f"{name}: not deterministic across two identical calls")
    if deep:
        fails += _check_pins(allocations, placed, name, rng, allocator)
    return fails, peak_of(placed), seconds


def _check_pins(
    allocations: tuple[Allocation, ...],
    placed: tuple[Allocation, ...],
    name: str,
    rng: random.Random,
    allocator: BaseAllocator,
) -> list[str]:
    """Honor pins, or refuse them; silently re-placing them is the failure."""
    pinned, pins = pin_subset(placed, 0.3, rng)
    if allocator.supports_pinned:
        try:
            repinned = tuple(fresh(name).allocate(pinned))
        except Exception as e:  # noqa: BLE001
            return [f"{name}[pinned]: raised {type(e).__name__}, {e}"]
        return check_placement(allocations, repinned, f"{name}[pinned]", pins)
    if not pins:
        return []
    try:
        fresh(name).allocate(pinned)
    except ValueError:
        return []
    return [f"{name}: accepted pins although supports_pinned is False"]


def run_cell(
    workload: Workload, size: int, seed: int, allocators: list[str], deep: bool
) -> dict:
    """One (workload, size, seed) cell: analysis first, then every allocator."""
    rng = random.Random(seed * 7919 + size)
    allocations = workload.allocations(size, seed)
    if not allocations:
        return {"workload": workload.name, "size": size, "seed": seed, "failures": []}

    count = len(allocations)
    fails = check_conflicts(allocations, brute=count <= BRUTE_LIMIT)
    fails += check_pressure(allocations, brute=count <= CLIQUE_LIMIT)
    fails += check_order_invariance(allocations, rng)
    fails += check_lane_invariance(allocations, rng)
    fails += check_linearize(allocations)
    fails += check_pickle(allocations)

    peaks: dict[str, int] = {}
    timings: dict[str, float] = {}
    for name in allocators:
        allocator_fails, peak, seconds = check_allocator(allocations, name, rng, deep)
        fails += allocator_fails
        if peak is not None:
            peaks[name] = peak
        if seconds is not None:
            timings[name] = seconds

    if deep and "omni" in peaks:
        fails += check_hierarchy(allocations, fresh("omni"))

    return {
        "workload": workload.name,
        "family": workload.family,
        "dim": allocations[0].dim,
        "size": count,
        "seed": seed,
        "bound": antichain_pressure(allocations),
        "known_optimum": workload.known_optimum,
        "peaks": peaks,
        "timings": timings,
        "failures": fails,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sizes", type=int, nargs="+", default=[1, 2, 3, 7, 33, 64, 257]
    )
    parser.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2])
    parser.add_argument("--allocators", nargs="+", default=None)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument(
        "--deep", action="store_true", help="also check pins and the hierarchy paths"
    )
    parser.add_argument("--out", type=Path, default=Path("fuzz_results_stress"))
    args = parser.parse_args()

    allocators = args.allocators or sorted(BaseAllocator.registry())
    cells = [
        (workload, size, seed)
        for workload in catalog()
        for size in args.sizes
        for seed in args.seeds
    ]
    print(f"{len(cells)} cells over {len(allocators)} allocators", flush=True)

    def work(cell: tuple[Workload, int, int]) -> dict:
        workload, size, seed = cell
        try:
            return run_cell(workload, size, seed, allocators, args.deep)
        except Exception:  # noqa: BLE001
            return {
                "workload": workload.name,
                "size": size,
                "seed": seed,
                "failures": [f"HARNESS: {traceback.format_exc(limit=6)}"],
            }

    results = []
    failures = 0
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for index, result in enumerate(pool.map(work, cells)):
            results.append(result)
            for failure in result["failures"]:
                failures += 1
                print(
                    f"FAIL [{result['workload']} n={result.get('size')} "
                    f"seed={result.get('seed')}] {failure}",
                    flush=True,
                )
            if (index + 1) % 100 == 0:
                print(
                    f"  {index + 1}/{len(cells)} cells, {failures} failures, "
                    f"{time.perf_counter() - started:.0f}s",
                    flush=True,
                )

    args.out.mkdir(parents=True, exist_ok=True)
    results_path = args.out / "results.json"
    results_path.write_text(json.dumps(results, indent=1, default=str))
    placements = sum(len(cell.get("peaks", {})) for cell in results)
    print(
        f"\n{len(cells)} cells, {placements} placements, {failures} failures, "
        f"{time.perf_counter() - started:.0f}s, wrote {results_path}"
    )
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
