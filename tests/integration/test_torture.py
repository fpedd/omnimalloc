#
# SPDX-License-Identifier: Apache-2.0
#

import json
import random
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from functools import cache
from pathlib import Path

try:
    import resource
except ModuleNotFoundError:  # Windows has no resource module
    resource = None

import pytest
from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.allocators import (
    BaseAllocator,
    BestFitAllocator,
    GeneticAllocator,
    GreedyAllocator,
    GreedyByAllAllocator,
    GreedyByAreaAllocator,
    GreedyByConflictAllocator,
    GreedyByConflictSizeAllocator,
    GreedyByDurationAllocator,
    GreedyBySizeAllocator,
    GreedyByStartAllocator,
    HillClimbAllocator,
    NaiveAllocator,
    OmniAllocator,
    RandomAllocator,
    SimulatedAnnealingAllocator,
    SupermallocAllocator,
    TabuSearchAllocator,
    TelamallocAllocator,
)
from omnimalloc.allocators.greedy import allocate_parallel
from omnimalloc.analysis import (
    antichain_pressure,
    conflict_graph,
    conflicts,
    placement_pressure,
)
from omnimalloc.benchmark.sources.pinwheel import PinwheelSource
from omnimalloc.benchmark.sources.tiling import TilingSource
from omnimalloc.common.constants import MB
from omnimalloc.primitives import Allocation
from omnimalloc.validate import validate_allocation

ANYTIME_ALLOCATORS = (
    HillClimbAllocator,
    GeneticAllocator,
    TabuSearchAllocator,
    SimulatedAnnealingAllocator,
)

PACKING_ALLOCATORS = (
    OmniAllocator(),
    GreedyAllocator(),
    GreedyBySizeAllocator(),
    GreedyByDurationAllocator(),
    GreedyByAreaAllocator(),
    GreedyByConflictAllocator(),
    GreedyByConflictSizeAllocator(),
    GreedyByStartAllocator(),
    GreedyByAllAllocator(),
    BestFitAllocator(),
    TelamallocAllocator(timeout=1.0),
    HillClimbAllocator(timeout=1.0),
    TabuSearchAllocator(timeout=1.0),
    SimulatedAnnealingAllocator(timeout=1.0),
    SupermallocAllocator(timeout=1.0),
)

SOAK_ALLOCATORS = (
    OmniAllocator(),
    GreedyAllocator(),
    GreedyBySizeAllocator(),
    GreedyByAreaAllocator(),
    GreedyByConflictAllocator(),
    BestFitAllocator(),
    NaiveAllocator(),
    RandomAllocator(),
    TelamallocAllocator(timeout=0.02),
    HillClimbAllocator(timeout=0.02, max_iterations=20),
    TabuSearchAllocator(timeout=0.02, max_iterations=20),
    SimulatedAnnealingAllocator(timeout=0.02, max_iterations=200),
    SupermallocAllocator(timeout=0.02),
)

TIMEOUT_BUDGET = 0.5
PLACEMENT_SLACK_MULTIPLE = 8
FIXED_SLACK = 0.4
SANE_PEAK_FACTOR = 3.0
RSS_GROWTH_LIMIT_KB = 32 * 1024
SOAK_STEPS = 300
SOAK_WARMUP_STEPS = 50
# Stacking is linear, so it places this in well under a second. The bound is
# what a quadratic implementation cannot meet whatever the host: the gap-scan
# form of the same function needs minutes here, so noise cannot close it.
STACKING_N = 100_000
STACKING_BUDGET = 10.0

CORE_COUNT_PROBE = """
import json
import random
import sys

from omnimalloc.allocators import (
    GeneticAllocator,
    GreedyByAllAllocator,
    HillClimbAllocator,
    OmniAllocator,
    SimulatedAnnealingAllocator,
    SupermallocAllocator,
    TabuSearchAllocator,
)
from omnimalloc.primitives import Allocation

rng = random.Random(11)
allocations = []
for i in range(120):
    start = rng.randint(0, 40)
    allocations.append(
        Allocation(
            id=i,
            size=rng.randint(1, 512),
            start=start,
            end=start + rng.randint(1, 12),
        )
    )
allocations = tuple(allocations)

threads = int(sys.argv[1])
seeded = {
    "omni": OmniAllocator(),
    "greedy_by_all": GreedyByAllAllocator(num_threads=threads),
    "hill_climb": HillClimbAllocator(timeout=None, max_iterations=60),
    "genetic": GeneticAllocator(timeout=None, max_generations=5, population_size=20),
    "tabu_search": TabuSearchAllocator(timeout=None, max_iterations=60),
    "simulated_annealing": SimulatedAnnealingAllocator(
        timeout=None, max_iterations=300
    ),
}
report = {
    name: [a.offset for a in allocator.allocate(allocations)]
    for name, allocator in seeded.items()
}
solved = SupermallocAllocator(timeout=None, num_threads=threads).solve(allocations)
report["supermalloc_verdict"] = [
    solved.peak,
    solved.lower_bound,
    solved.proved_optimal,
]
report["supermalloc_offsets"] = [a.offset for a in solved.allocations]
print(json.dumps(report))
"""


class FailingVariant:
    def allocate(self, _allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        raise RuntimeError("Variant failure")


def _dense_instance(num_allocations: int, seed: int) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    horizon = max(1, num_allocations // 4)
    span = max(1, num_allocations // 8)
    allocations = []
    for i in range(num_allocations):
        start = rng.randint(0, horizon)
        allocations.append(
            Allocation(
                id=i,
                size=rng.randint(1, 4096),
                start=start,
                end=start + rng.randint(1, span),
            )
        )
    return tuple(allocations)


def _small_instance(num_allocations: int, seed: int) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(num_allocations):
        start = rng.randint(0, 30)
        allocations.append(
            Allocation(
                id=i,
                size=rng.randint(1, 256),
                start=start,
                end=start + rng.randint(1, 9),
            )
        )
    return tuple(allocations)


def _soak_instance(rng: random.Random) -> tuple[Allocation, ...]:
    allocations = []
    for i in range(rng.randint(20, 120)):
        start = rng.randint(0, 60)
        allocations.append(
            Allocation(
                id=i,
                size=rng.randint(1, 8192),
                start=start,
                end=start + rng.randint(1, 15),
            )
        )
    return tuple(allocations)


def _one_placement_seconds(allocations: tuple[Allocation, ...]) -> float:
    placer = FirstFitPlacer(allocations)
    order = list(range(len(allocations)))
    placer.place(order)
    started = time.monotonic()
    for _ in range(3):
        placer.place(order)
    return (time.monotonic() - started) / 3


def _timeout_slack(allocations: tuple[Allocation, ...]) -> float:
    return FIXED_SLACK + PLACEMENT_SLACK_MULTIPLE * _one_placement_seconds(allocations)


def _rss_kb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def _current_rss_kb() -> int:
    statm = Path("/proc/self/statm")
    if not statm.exists():
        return _rss_kb()
    pages = int(statm.read_text().split()[1])
    return pages * (resource.getpagesize() // 1024)


@cache
def _pinned_probe(cores: str, num_threads: int) -> dict[str, list[object]]:
    taskset = shutil.which("taskset")
    completed = subprocess.run(
        [
            taskset,
            "-c",
            cores,
            sys.executable,
            "-c",
            CORE_COUNT_PROBE,
            str(num_threads),
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=True,
    )
    return json.loads(completed.stdout)


def _renumbered(
    allocations: tuple[Allocation, ...], id_base: int, time_shift: int
) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(
            id=id_base + i,
            size=a.size,
            start=a.start + time_shift,
            end=a.end + time_shift,
        )
        for i, a in enumerate(allocations)
    )


def _mixed_split_instance() -> tuple[Allocation, ...]:
    guillotine = TilingSource(num_allocations=97, capacity=MB, seed=5).get_allocations()
    pinwheel = PinwheelSource(num_allocations=97, capacity=MB, seed=5).get_allocations()
    return _renumbered(guillotine, 0, 0) + _renumbered(pinwheel, 10_000, 512 * 1024)


def _zipf_instance(num_allocations: int, seed: int) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(num_allocations):
        rank = rng.randint(1, num_allocations)
        start = rng.randint(0, num_allocations // 3)
        allocations.append(
            Allocation(
                id=i,
                size=max(1, 1_000_000 // rank),
                start=start,
                end=start + rng.randint(1, 10),
            )
        )
    return tuple(allocations)


def _just_over_half_instance(
    num_allocations: int, capacity: int, seed: int
) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(num_allocations):
        start = rng.randint(0, num_allocations // 4)
        allocations.append(
            Allocation(
                id=i,
                size=capacity // 2 + rng.randint(1, capacity // 100),
                start=start,
                end=start + rng.randint(1, 6),
            )
        )
    return tuple(allocations)


def _assert_sanely_packed(allocations: tuple[Allocation, ...]) -> None:
    lower_bound = antichain_pressure(allocations, work_budget=None)
    for allocator in PACKING_ALLOCATORS:
        placed = allocator.allocate(allocations)
        validate_allocation(placed)
        assert len(placed) == len(allocations)
        assert placement_pressure(placed) >= lower_bound, allocator
        assert placement_pressure(placed) <= SANE_PEAK_FACTOR * lower_bound, allocator


@pytest.mark.parametrize(
    "num_allocations", [1000, pytest.param(4000, marks=pytest.mark.slow)]
)
@pytest.mark.parametrize("allocator_cls", ANYTIME_ALLOCATORS)
def test_anytime_allocator_returns_within_its_timeout(
    allocator_cls: type[BaseAllocator], num_allocations: int
) -> None:
    allocations = _dense_instance(num_allocations, seed=7)
    slack = _timeout_slack(allocations)
    started = time.monotonic()
    placed = allocator_cls(timeout=TIMEOUT_BUDGET).allocate(allocations)
    elapsed = time.monotonic() - started
    assert len(placed) == num_allocations
    assert elapsed <= TIMEOUT_BUDGET + slack


@pytest.mark.parametrize(
    "num_allocations", [1000, pytest.param(4000, marks=pytest.mark.slow)]
)
def test_supermalloc_terminates_despite_its_uninterruptible_floor(
    num_allocations: int,
) -> None:
    allocations = _dense_instance(num_allocations, seed=7)
    started = time.monotonic()
    result = SupermallocAllocator(timeout=TIMEOUT_BUDGET).solve(allocations)
    elapsed = time.monotonic() - started
    assert len(result.allocations) == num_allocations
    assert result.peak >= result.lower_bound
    assert elapsed < 60.0


@pytest.mark.slow
def test_unbounded_timeout_is_not_capped_by_the_slack_bound() -> None:
    allocations = _dense_instance(1000, seed=7)
    slack = _timeout_slack(allocations)
    started = time.monotonic()
    placed = TabuSearchAllocator(timeout=None, max_iterations=40).allocate(allocations)
    elapsed = time.monotonic() - started
    assert len(placed) == 1000
    assert elapsed > TIMEOUT_BUDGET + slack


def test_conflicts_refuses_the_dense_instance_conflict_graph_survives() -> None:
    num_allocations = 10_000
    allocations = tuple(
        Allocation(id=i, size=1 + i % 7, start=0, end=1) for i in range(num_allocations)
    )
    with pytest.raises(RuntimeError, match="work_budget"):
        conflicts(allocations)
    graph = conflict_graph(allocations)
    assert len(graph) == num_allocations
    assert graph.pair_count == num_allocations * (num_allocations - 1) // 2
    assert graph.degree(0) == num_allocations - 1
    assert len(graph.neighbors(num_allocations - 1)) == num_allocations - 1


def test_conflicts_still_serves_an_instance_inside_its_budget() -> None:
    allocations = tuple(Allocation(id=i, size=8, start=0, end=1) for i in range(500))
    relation = conflicts(allocations)
    assert len(relation) == 500
    assert len(relation[0]) == 499


@pytest.mark.slow
def test_seeded_allocators_agree_across_core_counts() -> None:
    if shutil.which("taskset") is None:
        pytest.skip("taskset is unavailable")
    single = _pinned_probe("0", 1)
    quad = _pinned_probe("0-3", 4)
    seeded = (
        "omni",
        "greedy_by_all",
        "hill_climb",
        "genetic",
        "tabu_search",
        "simulated_annealing",
    )
    for name in seeded:
        assert single[name] == quad[name], name


def test_supermalloc_verdict_agrees_across_core_counts() -> None:
    if shutil.which("taskset") is None:
        pytest.skip("taskset is unavailable")
    single = _pinned_probe("0", 1)
    quad = _pinned_probe("0-3", 4)
    assert single["supermalloc_verdict"] == quad["supermalloc_verdict"]
    assert len(single["supermalloc_offsets"]) == len(quad["supermalloc_offsets"])


def test_supermalloc_verdict_is_stable_across_thread_counts() -> None:
    allocations = _small_instance(120, seed=11)
    results = [
        SupermallocAllocator(timeout=None, num_threads=threads).solve(allocations)
        for threads in (1, 2, 8)
    ]
    assert len({result.peak for result in results}) == 1
    assert len({result.lower_bound for result in results}) == 1
    assert all(result.proved_optimal for result in results)
    for result in results:
        validate_allocation(result.allocations)
        assert placement_pressure(result.allocations) == result.peak


def test_one_failing_variant_does_not_sink_the_parallel_call() -> None:
    allocations = _small_instance(80, seed=3)
    variants = (FailingVariant(), GreedyAllocator(), FailingVariant())
    placed = allocate_parallel(allocations, variants, num_threads=4)
    assert len(placed) == len(allocations)
    assert placement_pressure(placed) == placement_pressure(
        GreedyAllocator().allocate(allocations)
    )


def test_every_variant_failing_raises_runtime_error() -> None:
    allocations = _small_instance(80, seed=3)
    with pytest.raises(RuntimeError, match="Every allocator variant failed"):
        allocate_parallel(allocations, (FailingVariant(),) * 3, num_threads=4)


def test_serial_path_survives_a_failing_variant() -> None:
    allocations = _small_instance(80, seed=3)
    variants = (FailingVariant(), GreedyBySizeAllocator(), FailingVariant())
    placed = allocate_parallel(allocations, variants, num_threads=1)
    assert placement_pressure(placed) == placement_pressure(
        GreedyBySizeAllocator().allocate(allocations)
    )


def test_serial_path_raises_when_every_variant_fails() -> None:
    allocations = _small_instance(80, seed=3)
    with pytest.raises(RuntimeError, match="Every allocator variant failed"):
        allocate_parallel(allocations, (FailingVariant(),) * 3, num_threads=1)


def test_greedy_by_all_still_matches_its_best_variant() -> None:
    allocations = _small_instance(200, seed=8)
    best = min(
        placement_pressure(variant.allocate(allocations))
        for variant in (
            GreedyAllocator(),
            GreedyBySizeAllocator(),
            GreedyByDurationAllocator(),
            GreedyByAreaAllocator(),
            GreedyByConflictAllocator(),
            GreedyByConflictSizeAllocator(),
            GreedyByStartAllocator(),
        )
    )
    assert placement_pressure(GreedyByAllAllocator().allocate(allocations)) == best


def test_concurrent_genetic_calls_match_the_solo_result() -> None:
    allocations = _small_instance(80, seed=3)

    def run(_: int) -> list[int | None]:
        allocator = GeneticAllocator(
            timeout=None, max_generations=4, population_size=20
        )
        return [a.offset for a in allocator.allocate(allocations)]

    solo = run(0)
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(run, range(6)))
    assert all(result == solo for result in results)


def test_genetic_leaves_the_callers_global_random_stream_untouched() -> None:
    allocations = _small_instance(80, seed=3)
    random.seed(20250802)
    expected = [random.random() for _ in range(5)]
    random.seed(20250802)
    GeneticAllocator(timeout=None, max_generations=4, population_size=20).allocate(
        allocations
    )
    assert [random.random() for _ in range(5)] == expected


def test_concurrent_genetic_calls_leave_the_global_stream_untouched() -> None:
    allocations = _small_instance(80, seed=3)

    def run(_: int) -> None:
        GeneticAllocator(timeout=None, max_generations=4, population_size=20).allocate(
            allocations
        )

    random.seed(20250802)
    expected = [random.random() for _ in range(5)]
    random.seed(20250802)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(run, range(4)))
    assert [random.random() for _ in range(5)] == expected


def test_mixed_guillotine_and_pinwheel_splits_pack_sanely() -> None:
    _assert_sanely_packed(_mixed_split_instance())


def test_zipf_sized_instance_packs_sanely() -> None:
    _assert_sanely_packed(_zipf_instance(300, seed=4))


def test_just_over_half_capacity_pairs_pack_sanely() -> None:
    _assert_sanely_packed(_just_over_half_instance(200, capacity=4096, seed=6))


def test_naive_stacking_is_valid_but_stays_outside_the_sane_factor() -> None:
    allocations = _mixed_split_instance()
    placed = NaiveAllocator().allocate(allocations)
    validate_allocation(placed)
    assert placement_pressure(placed) == sum(a.size for a in allocations)
    assert placement_pressure(placed) > SANE_PEAK_FACTOR * antichain_pressure(
        allocations, work_budget=None
    )


def test_unpinned_stacking_stays_linear_at_scale() -> None:
    allocations = tuple(
        Allocation(id=i, size=64, start=i, end=i + 5) for i in range(STACKING_N)
    )
    start = time.perf_counter()
    placed = NaiveAllocator().allocate(allocations)
    assert time.perf_counter() - start < STACKING_BUDGET
    assert placement_pressure(placed) == sum(a.size for a in allocations)


def test_stacking_around_a_pin_still_places_every_allocation() -> None:
    pinned = Allocation(id="pinned", size=64, start=0, end=10, offset=0)
    free = tuple(Allocation(id=i, size=64, start=0, end=10) for i in range(64))
    placed = NaiveAllocator().allocate((pinned, *free))
    validate_allocation(placed)
    assert next(a.offset for a in placed if a.id == "pinned") == 0
    assert placement_pressure(placed) == 65 * 64


@pytest.mark.slow
@pytest.mark.skipif(resource is None, reason="resource is unavailable")
def test_soak_of_random_instances_keeps_memory_bounded() -> None:
    rng = random.Random(99)
    baseline = 0
    for step in range(SOAK_STEPS):
        allocations = _soak_instance(rng)
        antichain_pressure(allocations, work_budget=None)
        graph = conflict_graph(allocations)
        assert len(graph) == len(allocations)
        for allocator in SOAK_ALLOCATORS:
            placed = allocator.allocate(allocations)
            validate_allocation(placed)
            assert len(placed) == len(allocations)
        if step == SOAK_WARMUP_STEPS - 1:
            baseline = _current_rss_kb()
    assert _current_rss_kb() - baseline < RSS_GROWTH_LIMIT_KB
