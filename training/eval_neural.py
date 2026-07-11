#
# SPDX-License-Identifier: Apache-2.0
#

"""Evaluate the NeuralAllocator against the classic allocators.

Runs every allocator on held-out generator instances (seeds disjoint from
training), the bundled minimalloc CSV datasets, and known-optimum tiling
problems. Reports mean peak/LB, win counts, and wall-clock time.

Usage:
    uv run python training/eval_neural.py [--weights path] [--quick]
"""

import argparse
import random
import time
from collections import defaultdict
from pathlib import Path

from omnimalloc.allocators import (
    BaseAllocator,
    BestFitAllocator,
    GreedyByAllAllocatorCpp,
    GreedyBySizeAllocator,
    HillClimbAllocator,
    NeuralAllocator,
    SimulatedAnnealingAllocator,
    SupermallocAllocator,
    TelamallocAllocator,
)
from omnimalloc.allocators.greedy_base import peak_memory
from omnimalloc.allocators.neural import DEFAULT_WEIGHTS, peak_lower_bound
from omnimalloc.benchmark.sources import MinimallocSource, MinimallocSubset
from omnimalloc.primitives import Allocation


def held_out_instances(quick: bool) -> dict[str, list[tuple[Allocation, ...]]]:
    """Held-out problems: generator seeds >= 10^9 are never used in training."""
    import sys

    sys.path.insert(0, str(Path(__file__).parent))
    from train_neural import _random_source  # type: ignore

    rng = random.Random(1_000_000_007)
    sets: dict[str, list[tuple[Allocation, ...]]] = defaultdict(list)
    count = 8 if quick else 24
    for size in (50, 100) if quick else (50, 100, 200):
        for _ in range(count):
            source = _random_source(rng, size, seed=rng.randrange(1 << 30) + (1 << 30))
            sets[f"generators_n{size}"].append(source.get_allocations())

    for subset in (
        (MinimallocSubset.SMALL,)
        if quick
        else (MinimallocSubset.SMALL, MinimallocSubset.CHALLENGING)
    ):
        source = MinimallocSource(subset=subset)
        for variant in source.get_available_variants():
            sets[f"minimalloc_{subset.value}"].append(
                source.get_variant(variant).allocations
            )
    return sets


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weights", type=str, default=str(DEFAULT_WEIGHTS))
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()

    allocators: dict[str, BaseAllocator] = {
        "neural (decode only)": NeuralAllocator(
            weights=args.weights, num_samples=0, portfolio=False
        ),
        "neural (sampling only)": NeuralAllocator(
            weights=args.weights, portfolio=False
        ),
        "neural (full)": NeuralAllocator(weights=args.weights),
        "greedy_by_size": GreedyBySizeAllocator(),
        "greedy_by_all_cpp": GreedyByAllAllocatorCpp(),
        "best_fit": BestFitAllocator(),
        "telamalloc": TelamallocAllocator(),
        "hillclimb": HillClimbAllocator(),
        "simulated_annealing": SimulatedAnnealingAllocator(),
        "supermalloc": SupermallocAllocator(),
    }

    sets = held_out_instances(args.quick)
    total_gap: dict[str, list[float]] = defaultdict(list)
    total_time: dict[str, float] = defaultdict(float)
    wins: dict[str, int] = defaultdict(int)

    for set_name, problems in sets.items():
        gaps: dict[str, list[float]] = defaultdict(list)
        for allocations in problems:
            lb = max(peak_lower_bound(allocations), 1)
            peaks: dict[str, int] = {}
            for name, allocator in allocators.items():
                start = time.monotonic()
                placed = allocator.allocate(allocations)
                total_time[name] += time.monotonic() - start
                peaks[name] = peak_memory(placed)
                gaps[name].append(peaks[name] / lb)
                total_gap[name].append(peaks[name] / lb)
            best = min(peaks.values())
            for name, peak in peaks.items():
                if peak == best:
                    wins[name] += 1

        print(f"\n=== {set_name} ({len(problems)} problems) ===")
        for name in sorted(gaps, key=lambda n: sum(gaps[n])):
            mean_gap = sum(gaps[name]) / len(gaps[name])
            print(f"  {name:28s} mean peak/LB: {mean_gap:.4f}")

    print(f"\n=== overall ({sum(len(p) for p in sets.values())} problems) ===")
    print(f"  {'allocator':28s} {'mean peak/LB':>14s} {'wins':>6s} {'time':>9s}")
    for name in sorted(total_gap, key=lambda n: sum(total_gap[n])):
        mean_gap = sum(total_gap[name]) / len(total_gap[name])
        print(f"  {name:28s} {mean_gap:14.4f} {wins[name]:6d} {total_time[name]:8.1f}s")


if __name__ == "__main__":
    main()
