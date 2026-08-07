#
# SPDX-License-Identifier: Apache-2.0
#
"""Cross-checks that pit one part of the API against another.

Each is an oracle the implementation cannot satisfy by being self-consistently
wrong: a proved optimum against every rival, a linearization against its source.
"""

import argparse
import random
import re
import sys
import time
from collections.abc import Callable

from invariants import brute_collisions, peak_of
from omnimalloc import allocate, validate_allocation
from omnimalloc._cpp import FirstFitPlacer, max_threads, set_max_threads
from omnimalloc.allocators import (
    BaseAllocator,
    GeneticAllocator,
    HillClimbAllocator,
    SimulatedAnnealingAllocator,
    SupermallocAllocator,
    TabuSearchAllocator,
    TelamallocAllocator,
)
from omnimalloc.analysis import (
    antichain_pressure,
    conflict_degrees,
    conflict_graph,
    try_linearize,
)
from workloads import Workload, by_name, catalog

SCALAR_RIVALS = (
    "omni",
    "greedy",
    "greedy_by_size",
    "greedy_by_area",
    "greedy_by_duration",
    "greedy_by_conflict",
    "greedy_by_conflict_size",
    "greedy_by_start",
    "greedy_by_all",
    "best_fit",
    "minimalloc",
    "telamalloc",
)

# Built by hand rather than through the registry: only the concrete classes
# declare a `timeout`, which is the parameter under test here.
TIMED: tuple[tuple[str, Callable[[float], BaseAllocator]], ...] = (
    ("supermalloc", lambda t: SupermallocAllocator(timeout=t)),
    ("telamalloc", lambda t: TelamallocAllocator(timeout=t)),
    ("simulated_annealing", lambda t: SimulatedAnnealingAllocator(timeout=t)),
    ("tabu_search", lambda t: TabuSearchAllocator(timeout=t)),
    ("hill_climb", lambda t: HillClimbAllocator(timeout=t)),
    ("genetic", lambda t: GeneticAllocator(timeout=t)),
)


class Report:
    """Collects failures so one broken oracle does not hide the others."""

    def __init__(self) -> None:
        self.failures: list[str] = []

    def fail(self, check: str, message: str) -> None:
        self.failures.append(f"{check}: {message}")
        print(f"FAIL {check}: {message}", flush=True)


def scalar_workloads() -> list[Workload]:
    return [w for w in catalog() if w.dim == 1 and "fixed" not in w.tags]


def check_optimality(
    report: Report, sizes: tuple[int, ...], seeds: tuple[int, ...]
) -> None:
    """A proved optimum must be unbeatable and bracketed by the exact bound."""
    checked = proved = 0
    for workload in scalar_workloads():
        for size in sizes:
            for seed in seeds:
                allocations = workload.allocations(size, seed)
                if not allocations:
                    continue
                checked += 1
                where = f"{workload.name} n={size} seed={seed}"
                proved += _check_solution(report, allocations, where)
    print(f"  optimality: {checked} instances, {proved} proved optimal", flush=True)


def _check_solution(report: Report, allocations: tuple, where: str) -> int:
    """Check one solved instance; returns 1 when the search proved optimality."""
    result = SupermallocAllocator(timeout=5.0).solve(allocations)
    bound = antichain_pressure(allocations)

    if brute_collisions(result.allocations):
        report.fail("optimality", f"{where}: invalid placement")
    if result.peak != peak_of(result.allocations):
        report.fail("optimality", f"{where}: reported peak is not the peak")
    if result.lower_bound > result.peak:
        report.fail("optimality", f"{where}: lower_bound above its own peak")
    if result.lower_bound > bound:
        report.fail("optimality", f"{where}: bound above the exact bound")
    if not result.proved_optimal:
        return 0
    if result.peak < bound:
        report.fail("optimality", f"{where}: optimum below the exact bound")
    _check_unbeaten(report, allocations, result.peak, where)
    return 1


def _check_unbeaten(report: Report, allocations: tuple, peak: int, where: str) -> None:
    """No allocator may undercut a peak the search proved optimal."""
    for name in SCALAR_RIVALS:
        allocator = BaseAllocator.get(name)()
        if not allocator.supports(allocations):
            continue
        try:
            rival = tuple(allocator.allocate(allocations))
        except (ImportError, RuntimeError) as e:
            # minimalloc wraps a package that need not be installed
            print(f"  skipped {name} on {where}: {e}", flush=True)
            continue
        if peak_of(rival) < peak:
            report.fail(
                "optimality",
                f"{where}: {name} reached {peak_of(rival)} below the proved {peak}",
            )


def check_linearize_transfer(
    report: Report, sizes: tuple[int, ...], seeds: tuple[int, ...]
) -> None:
    """Offsets found on a linearization must stay valid on the vector original."""
    transferred = 0
    for workload in catalog():
        if workload.dim < 2:
            continue
        for size in sizes:
            for seed in seeds:
                allocations = workload.allocations(size, seed)
                linearized = try_linearize(allocations)
                if not allocations or linearized is None:
                    continue
                transferred += 1
                placed = BaseAllocator.get("greedy_by_size")().allocate(linearized)
                offsets = {a.id: a.offset for a in placed}
                moved = tuple(a.with_offset(offsets[a.id]) for a in allocations)
                where = f"{workload.name} n={size} seed={seed}"
                if brute_collisions(moved):
                    report.fail(
                        "linearize_transfer",
                        f"{where}: offsets valid on the linearization collide "
                        f"on the original",
                    )
                try:
                    validate_allocation(moved)
                except ValueError as e:
                    report.fail("linearize_transfer", f"{where}: {e}")
    print(f"  linearize_transfer: {transferred} linearizable instances", flush=True)


def check_idempotence(report: Report, sizes: tuple[int, ...]) -> None:
    """Re-running a pinning allocator on its own output must change nothing."""
    for workload in catalog():
        for size in sizes:
            allocations = workload.allocations(size, 0)
            if not allocations:
                continue
            for name in ("omni", "greedy_by_size", "best_fit"):
                allocator = BaseAllocator.get(name)()
                if not allocator.supports(allocations):
                    continue
                once = tuple(allocator.allocate(allocations))
                twice = tuple(allocator.allocate(once))
                if [a.offset for a in twice] != [a.offset for a in once]:
                    report.fail(
                        "idempotence",
                        f"{name} on {workload.name} n={size} moved a full pin",
                    )
    print("  idempotence: done", flush=True)


def check_monotonicity(report: Report, sizes: tuple[int, ...]) -> None:
    """A bound never falls when allocations are added."""
    rng = random.Random(11)
    for workload in catalog():
        for size in sizes:
            allocations = workload.allocations(size, 0)
            if len(allocations) < 4:
                continue
            keep = sorted(rng.sample(range(len(allocations)), len(allocations) // 2))
            subset = tuple(allocations[i] for i in keep)
            full = antichain_pressure(allocations)
            partial = antichain_pressure(subset)
            if partial > full:
                report.fail(
                    "monotonicity",
                    f"{workload.name} n={size}: subset bound {partial} above {full}",
                )
    print("  monotonicity: done", flush=True)


def check_budget_boundary(report: Report) -> None:
    """The budget an error advertises must be exactly the one that admits it."""
    allocations = by_name()["sync[dense,t=8]"].allocations(400, 0)
    try:
        antichain_pressure(allocations, work_budget=0)
    except RuntimeError as e:
        match = re.search(r"work_budget=(\d+)", str(e))
        if match is None:
            report.fail("budget_boundary", f"no budget advertised in {e!r}")
            return
        advertised = int(match.group(1))
        try:
            antichain_pressure(allocations, work_budget=advertised)
        except RuntimeError:
            report.fail(
                "budget_boundary", f"advertised {advertised} still refuses the instance"
            )
        try:
            antichain_pressure(allocations, work_budget=advertised - 1)
            report.fail(
                "budget_boundary", f"{advertised - 1} accepted below the advertised"
            )
        except RuntimeError:
            pass
        print(f"  budget_boundary: advertised {advertised} is exact", flush=True)
    else:
        report.fail("budget_boundary", "work_budget=0 admitted a vector instance")


def check_max_entries_boundary(report: Report) -> None:
    """The CSR ceiling must admit exactly what the relation needs, and no less."""
    allocations = by_name()["high_contention"].allocations(300, 0)
    entries = 2 * conflict_graph(allocations).pair_count
    try:
        conflict_graph(allocations, max_entries=entries)
    except RuntimeError as e:
        report.fail("max_entries", f"the exact ceiling {entries} was refused, {e}")
    try:
        conflict_graph(allocations, max_entries=entries - 1)
        report.fail("max_entries", f"{entries - 1} accepted below the need {entries}")
    except RuntimeError:
        pass
    print(f"  max_entries: exact ceiling {entries} honored", flush=True)


def check_timeouts(report: Report, size: int, budget: float) -> None:
    """A declared timeout must bound the call, within a generous slack."""
    allocations = by_name()["random"].allocations(size, 0)
    for name, make in TIMED:
        started = time.perf_counter()
        make(budget).allocate(allocations)
        elapsed = time.perf_counter() - started
        print(f"  timeout {name}: {elapsed:.2f}s against {budget:.1f}s", flush=True)
        if elapsed > budget * 5 + 2.0:
            report.fail("timeouts", f"{name} took {elapsed:.1f}s on {budget}s")


def check_thread_invariance(report: Report) -> None:
    """Results must not depend on how many workers the kernels use."""
    original = max_threads()
    try:
        for name in ("random", "sync[dense,t=8]", "tiling"):
            allocations = by_name()[name].allocations(2000, 0)
            reference: dict[str, object] = {}
            for threads in (1, 2, 4, 8, 32):
                set_max_threads(threads)
                observed: dict[str, object] = {
                    "omni": [a.offset for a in allocate(allocations, "omni")],
                    "antichain": antichain_pressure(allocations),
                    "degrees": conflict_degrees(allocations),
                }
                if not reference:
                    reference = observed
                    continue
                for key, value in observed.items():
                    if value != reference[key]:
                        report.fail(
                            "thread_invariance", f"{name}: {key} differs at {threads}"
                        )
    finally:
        set_max_threads(original)
    print("  thread_invariance: done", flush=True)


def check_pins(report: Report, sizes: tuple[int, ...]) -> None:
    """A pin survives every fraction, and the rest still pack around it."""
    rng = random.Random(5)
    allocator = BaseAllocator.get("omni")()
    for workload in catalog():
        for size in sizes:
            allocations = workload.allocations(size, 0)
            if not allocations:
                continue
            placed = tuple(allocator.allocate(allocations))
            if [a.offset for a in allocator.allocate(placed)] != [
                a.offset for a in placed
            ]:
                report.fail("pins", f"{workload.name} n={size}: a full pin moved")
            for fraction in (0.25, 0.5, 0.75):
                partial = tuple(
                    a if rng.random() < fraction else a.with_offset(None)
                    for a in placed
                )
                pins = {a.id: a.offset for a in partial if a.offset is not None}
                result = tuple(allocator.allocate(partial))
                moved = [a.id for a in result if pins.get(a.id) not in (None, a.offset)]
                if moved:
                    report.fail("pins", f"{workload.name} n={size}: {moved[0]!r} moved")
                if brute_collisions(result):
                    report.fail(
                        "pins", f"{workload.name} n={size} f={fraction}: collisions"
                    )
    print("  pins: done", flush=True)


def check_first_fit_placer(report: Report, sizes: tuple[int, ...]) -> None:
    """``FirstFitPlacer.peak(order)`` must agree with placing that same order."""
    rng = random.Random(3)
    for workload in scalar_workloads():
        for size in sizes:
            allocations = workload.allocations(size, 0)
            if not allocations:
                continue
            placer = FirstFitPlacer(allocations)
            for _ in range(3):
                order = list(range(len(allocations)))
                rng.shuffle(order)
                predicted = placer.peak(order)
                placed = placer.place(order)
                if peak_of(placed) != predicted:
                    report.fail(
                        "first_fit_placer",
                        f"{workload.name} n={size}: peak() {predicted} != "
                        f"place() {peak_of(placed)}",
                    )
                if brute_collisions(placed):
                    report.fail(
                        "first_fit_placer", f"{workload.name} n={size}: collisions"
                    )
    print("  first_fit_placer: done", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=[12, 40, 120])
    parser.add_argument("--seeds", type=int, nargs="+", default=[0, 1])
    parser.add_argument("--timeout-size", type=int, default=3000)
    parser.add_argument("--timeout-budget", type=float, default=1.0)
    args = parser.parse_args()
    sizes, seeds = tuple(args.sizes), tuple(args.seeds)
    report = Report()

    checks = (
        ("optimality", lambda: check_optimality(report, sizes, seeds)),
        ("linearize_transfer", lambda: check_linearize_transfer(report, sizes, seeds)),
        ("idempotence", lambda: check_idempotence(report, sizes)),
        ("monotonicity", lambda: check_monotonicity(report, sizes)),
        ("budget_boundary", lambda: check_budget_boundary(report)),
        ("max_entries_boundary", lambda: check_max_entries_boundary(report)),
        (
            "timeouts",
            lambda: check_timeouts(report, args.timeout_size, args.timeout_budget),
        ),
        ("thread_invariance", lambda: check_thread_invariance(report)),
        ("pins", lambda: check_pins(report, sizes)),
        ("first_fit_placer", lambda: check_first_fit_placer(report, sizes)),
    )
    for name, check in checks:
        print(f"== {name}", flush=True)
        started = time.perf_counter()
        check()
        print(f"== {name} took {time.perf_counter() - started:.0f}s", flush=True)

    print(f"\n{len(report.failures)} oracle failures")
    sys.exit(1 if report.failures else 0)


if __name__ == "__main__":
    main()
