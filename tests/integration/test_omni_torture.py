#
# SPDX-License-Identifier: Apache-2.0
#

import itertools
import random
from concurrent.futures import ThreadPoolExecutor

import pytest
from omnimalloc._cpp import FirstFitPlacer, compute_temporal_overlaps
from omnimalloc.allocators import OmniAllocator
from omnimalloc.allocators.greedy_cpp import (
    GreedyAllocatorCpp,
    GreedyByAreaAllocatorCpp,
    GreedyByConflictAllocatorCpp,
    GreedyByConflictSizeAllocatorCpp,
    GreedyByDurationAllocatorCpp,
    GreedyBySizeAllocatorCpp,
    GreedyByStartAllocatorCpp,
)
from omnimalloc.benchmark.sources.concurrent_tiling import ConcurrentTilingSource
from omnimalloc.benchmark.sources.generator import HighContentionSource, RandomSource
from omnimalloc.benchmark.sources.pinwheel import PinwheelSource
from omnimalloc.benchmark.sources.sync_patterns import SYNC_PATTERNS, SyncPatternSource
from omnimalloc.benchmark.sources.tiling import TilingSource
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.primitives.pressure import (
    get_closure_pressure,
    get_per_allocation_placement_pressure,
    get_pressure,
)
from omnimalloc.validate import validate_allocation

GREEDY_PORTFOLIO = (
    GreedyAllocatorCpp,
    GreedyBySizeAllocatorCpp,
    GreedyByDurationAllocatorCpp,
    GreedyByAreaAllocatorCpp,
    GreedyByConflictAllocatorCpp,
    GreedyByConflictSizeAllocatorCpp,
    GreedyByStartAllocatorCpp,
)


def _peak(allocations: tuple[Allocation, ...]) -> int:
    return max(a.offset + a.size for a in allocations)


def _assert_conflicting_pairs_disjoint(placed: tuple[Allocation, ...]) -> None:
    by_id = {a.id: a for a in placed}
    for alloc_id, neighbor_ids in compute_temporal_overlaps(placed).items():
        a = by_id[alloc_id]
        for neighbor_id in neighbor_ids:
            b = by_id[neighbor_id]
            assert a.offset + a.size <= b.offset or b.offset + b.size <= a.offset


def _certify(
    allocations: tuple[Allocation, ...], placed: tuple[Allocation, ...]
) -> int:
    peak = _peak(placed)
    assert get_pressure(allocations, work_budget=None) <= peak
    assert peak <= sum(a.size for a in allocations)
    assert max(get_per_allocation_placement_pressure(placed).values()) == peak
    _assert_conflicting_pairs_disjoint(placed)
    return peak


def _random_scalar(n: int, seed: int, horizon: int = 50) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(n):
        start = rng.randint(0, horizon)
        allocations.append(
            Allocation(
                id=i,
                size=rng.randint(1, 128),
                start=start,
                end=start + rng.randint(1, 20),
            )
        )
    return tuple(allocations)


def _random_vector_instance(rng: random.Random) -> tuple[Allocation, ...]:
    dim = rng.choice((2, 3))
    allocations = []
    for i in range(rng.randint(4, 7)):
        start = tuple(rng.randint(0, 4) for _ in range(dim))
        delta = [rng.randint(0, 3) for _ in range(dim)]
        if sum(delta) == 0:
            delta[rng.randrange(dim)] = 1
        end = tuple(s + d for s, d in zip(start, delta, strict=True))
        allocations.append(
            Allocation(id=i, size=rng.randint(1, 100), start=start, end=end)
        )
    return tuple(allocations)


def _tiled_crowns(num_crowns: int) -> tuple[Allocation, ...]:
    allocations = []
    for j in range(num_crowns):
        b = 3 * j
        allocations.extend(
            (
                Allocation(id=4 * j, size=8, start=(b, b), end=(b + 1, b)),
                Allocation(id=4 * j + 1, size=16, start=(b + 1, b), end=(b + 2, b)),
                Allocation(id=4 * j + 2, size=32, start=(b, b), end=(b, b + 1)),
                Allocation(id=4 * j + 3, size=64, start=(b, b + 1), end=(b, b + 2)),
            )
        )
    return tuple(allocations)


@pytest.mark.parametrize("pattern", SYNC_PATTERNS)
@pytest.mark.parametrize("num_threads", [2, 4])
def test_sync_pattern_placement_is_certified(pattern: str, num_threads: int) -> None:
    source = SyncPatternSource(
        num_allocations=64, num_threads=num_threads, pattern=pattern
    )
    allocations = source.get_allocations()
    placed = OmniAllocator().allocate(allocations)
    validate_allocation(Pool(id="p", allocations=placed))
    _certify(allocations, placed)


@pytest.mark.parametrize("pattern", SYNC_PATTERNS)
def test_closure_antichain_peak_chain_on_sync_patterns(pattern: str) -> None:
    source = SyncPatternSource(num_allocations=32, num_threads=3, pattern=pattern)
    allocations = source.get_allocations()
    peak = _peak(OmniAllocator().allocate(allocations))
    closure = get_closure_pressure(allocations, closure_cap=1 << 18)
    antichain = get_pressure(allocations, work_budget=None)
    assert closure <= antichain <= peak
    assert get_pressure(allocations) == antichain


@pytest.mark.parametrize("num_threads", [2, 4])
@pytest.mark.parametrize("num_syncs", [0, 16, 256])
def test_concurrent_tiling_is_certified_near_optimum(
    num_threads: int, num_syncs: int
) -> None:
    capacity = 1024 * 1024
    source = ConcurrentTilingSource(
        num_allocations=96,
        num_threads=num_threads,
        num_syncs=num_syncs,
        capacity=capacity,
    )
    allocations = source.get_allocations()
    placed = OmniAllocator().allocate(allocations)
    peak = _certify(allocations, placed)
    assert get_pressure(allocations, work_budget=None) <= capacity
    assert capacity <= peak <= 2 * capacity


@pytest.mark.parametrize("source_cls", [TilingSource, PinwheelSource])
@pytest.mark.parametrize("num_allocations", [128, 512])
def test_scalar_tiling_peak_stays_near_known_optimum(
    source_cls: type[TilingSource] | type[PinwheelSource], num_allocations: int
) -> None:
    capacity = 1024 * 1024
    source = source_cls(num_allocations=num_allocations, capacity=capacity)
    allocations = source.get_allocations()
    placed = OmniAllocator().allocate(allocations)
    peak = _certify(allocations, placed)
    assert capacity <= peak <= 2 * capacity


def test_scalar_portfolio_not_worse_than_any_greedy_variant() -> None:
    sources = (
        RandomSource(num_allocations=200, seed=5),
        HighContentionSource(num_allocations=200, time_window=12, seed=5),
        TilingSource(num_allocations=256, seed=5),
    )
    for source in sources:
        allocations = source.get_allocations()
        omni_peak = _peak(OmniAllocator().allocate(allocations))
        for variant_cls in GREEDY_PORTFOLIO:
            assert omni_peak <= _peak(variant_cls().allocate(allocations))


def test_vector_not_worse_than_input_order_greedy() -> None:
    for pattern in ("independent", "ring", "barrier", "dense"):
        source = SyncPatternSource(num_allocations=96, num_threads=5, pattern=pattern)
        allocations = source.get_allocations()
        omni_peak = _peak(OmniAllocator().allocate(allocations))
        assert omni_peak <= _peak(GreedyAllocatorCpp().allocate(allocations))


def test_size_scaling_scales_peak_linearly() -> None:
    source = SyncPatternSource(num_allocations=80, num_threads=4, pattern="sparse")
    allocations = source.get_allocations()
    scaled = tuple(
        Allocation(id=a.id, size=7 * a.size, start=a.start, end=a.end)
        for a in allocations
    )
    assert _peak(OmniAllocator().allocate(scaled)) == 7 * _peak(
        OmniAllocator().allocate(allocations)
    )


def test_clock_translation_preserves_offsets() -> None:
    source = SyncPatternSource(num_allocations=80, num_threads=4, pattern="groups")
    allocations = source.get_allocations()
    translated = tuple(
        Allocation(
            id=a.id,
            size=a.size,
            start=tuple(t + 1000 for t in a.start),
            end=tuple(t + 1000 for t in a.end),
        )
        for a in allocations
    )
    assert [x.offset for x in OmniAllocator().allocate(translated)] == [
        x.offset for x in OmniAllocator().allocate(allocations)
    ]


def test_lane_permutation_preserves_exact_pressures() -> None:
    source = SyncPatternSource(num_allocations=48, num_threads=4, pattern="fork_join")
    allocations = source.get_allocations()
    lanes = (2, 0, 3, 1)
    permuted = tuple(
        Allocation(
            id=a.id,
            size=a.size,
            start=tuple(a.start[lane] for lane in lanes),
            end=tuple(a.end[lane] for lane in lanes),
        )
        for a in allocations
    )
    assert get_pressure(permuted, work_budget=None) == get_pressure(
        allocations, work_budget=None
    )
    assert get_closure_pressure(permuted, closure_cap=1 << 18) == get_closure_pressure(
        allocations, closure_cap=1 << 18
    )
    _certify(permuted, OmniAllocator().allocate(permuted))


def test_zero_lane_padding_preserves_offsets() -> None:
    source = SyncPatternSource(num_allocations=64, num_threads=3, pattern="ring")
    allocations = source.get_allocations()
    padded = tuple(
        Allocation(id=a.id, size=a.size, start=(*a.start, 0, 0), end=(*a.end, 0, 0))
        for a in allocations
    )
    assert [x.offset for x in OmniAllocator().allocate(padded)] == [
        x.offset for x in OmniAllocator().allocate(allocations)
    ]


@pytest.mark.parametrize("dim", [2, 8])
def test_lockstep_embedding_matches_scalar_peak(dim: int) -> None:
    allocations = _random_scalar(300, seed=9)
    lockstep = tuple(
        Allocation(id=a.id, size=a.size, start=(a.start,) * dim, end=(a.end,) * dim)
        for a in allocations
    )
    assert _peak(OmniAllocator().allocate(lockstep)) == _peak(
        OmniAllocator().allocate(allocations)
    )


@pytest.mark.parametrize("dim", [2, 8, 32])
def test_one_hot_lanes_reach_the_exact_optimum(dim: int) -> None:
    size = 64
    allocations = tuple(
        Allocation(
            id=lane * 1000 + step,
            size=size,
            start=tuple(step if t == lane else 0 for t in range(dim)),
            end=tuple(step + 1 if t == lane else 0 for t in range(dim)),
        )
        for lane in range(dim)
        for step in range(8)
    )
    placed = OmniAllocator().allocate(allocations)
    peak = _certify(allocations, placed)
    assert peak == dim * size
    assert get_pressure(allocations, work_budget=None) == dim * size


def test_total_order_chain_peak_is_max_size() -> None:
    rng = random.Random(3)
    sizes = [rng.randint(1, 4096) for _ in range(1000)]
    scalar = tuple(
        Allocation(id=i, size=s, start=i, end=i + 1) for i, s in enumerate(sizes)
    )
    lockstep = tuple(
        Allocation(id=i, size=s, start=(i,) * 4, end=(i + 1,) * 4)
        for i, s in enumerate(sizes)
    )
    assert _peak(OmniAllocator().allocate(scalar)) == max(sizes)
    assert _peak(OmniAllocator().allocate(lockstep)) == max(sizes)


def test_full_antichain_peak_is_total_size() -> None:
    rng = random.Random(4)
    sizes = [rng.randint(1, 4096) for _ in range(512)]
    allocations = tuple(
        Allocation(id=i, size=s, start=0, end=1) for i, s in enumerate(sizes)
    )
    placed = OmniAllocator().allocate(allocations)
    assert _peak(placed) == sum(sizes)
    _assert_conflicting_pairs_disjoint(placed)


def test_identical_vector_lifetimes_stack_to_total_size() -> None:
    allocations = tuple(
        Allocation(id=i, size=8 + i % 5, start=(1, 1, 1), end=(2, 2, 2))
        for i in range(512)
    )
    placed = OmniAllocator().allocate(allocations)
    assert _peak(placed) == sum(a.size for a in allocations)
    _assert_conflicting_pairs_disjoint(placed)


def test_extreme_clock_values_and_sizes_place_validly() -> None:
    big = 10**18
    allocations = tuple(
        Allocation(
            id=i,
            size=2**40 + i,
            start=(big + i, big - i),
            end=(big + i + 1, big - i + 1),
        )
        for i in range(16)
    )
    placed = OmniAllocator().allocate(allocations)
    peak = _certify(allocations, placed)
    assert peak == sum(a.size for a in allocations)


def test_tiled_crowns_are_certified_and_exact() -> None:
    allocations = _tiled_crowns(50)
    assert get_pressure(allocations, work_budget=None) == 80
    assert get_pressure(allocations) == 80
    placed = OmniAllocator().allocate(allocations)
    assert _certify(allocations, placed) >= 80


def test_pressure_budget_raises_but_default_succeeds_on_crowns() -> None:
    allocations = _tiled_crowns(50)
    with pytest.raises(RuntimeError, match="work_budget"):
        get_pressure(allocations, work_budget=1)
    assert get_pressure(allocations) == get_pressure(allocations, work_budget=None)


def test_exhaustive_first_fit_orders_bracket_omni_peak() -> None:
    rng = random.Random(21)
    for _ in range(15):
        allocations = _random_vector_instance(rng)
        placer = FirstFitPlacer(list(allocations))
        best = min(
            placer.evaluate(list(order))
            for order in itertools.permutations(range(len(allocations)))
        )
        omni_peak = _peak(OmniAllocator().allocate(allocations))
        closure = get_closure_pressure(allocations)
        antichain = get_pressure(allocations, work_budget=None)
        assert closure <= antichain <= best <= omni_peak


@pytest.mark.slow
@pytest.mark.parametrize("pattern", SYNC_PATTERNS)
@pytest.mark.parametrize("num_threads", [8, 16])
@pytest.mark.parametrize("seed", [0, 1, 2])
def test_torture_sync_pattern_grid(pattern: str, num_threads: int, seed: int) -> None:
    source = SyncPatternSource(
        num_allocations=2048, num_threads=num_threads, pattern=pattern, seed=seed
    )
    allocations = source.get_allocations()
    placed = OmniAllocator().allocate(allocations)
    _certify(allocations, placed)


@pytest.mark.slow
@pytest.mark.parametrize("num_threads", [4, 8])
@pytest.mark.parametrize("num_syncs", [0, 64, 4096])
def test_torture_concurrent_tiling_scale(num_threads: int, num_syncs: int) -> None:
    capacity = 1024 * 1024
    source = ConcurrentTilingSource(
        num_allocations=8192,
        num_threads=num_threads,
        num_syncs=num_syncs,
        capacity=capacity,
    )
    allocations = source.get_allocations()
    placed = OmniAllocator().allocate(allocations)
    peak = _certify(allocations, placed)
    assert get_pressure(allocations, work_budget=None) <= capacity
    assert capacity <= peak <= 2 * capacity


@pytest.mark.slow
def test_torture_lockstep_scale_matches_scalar() -> None:
    allocations = _random_scalar(20000, seed=17, horizon=20000)
    lockstep = tuple(
        Allocation(id=a.id, size=a.size, start=(a.start,) * 32, end=(a.end,) * 32)
        for a in allocations
    )
    scalar_placed = OmniAllocator().allocate(allocations)
    lockstep_placed = OmniAllocator().allocate(lockstep)
    assert _peak(lockstep_placed) == _peak(scalar_placed)
    _assert_conflicting_pairs_disjoint(lockstep_placed)


@pytest.mark.slow
def test_torture_sparse_sync_at_scale_is_certified() -> None:
    source = SyncPatternSource(
        num_allocations=8000, num_threads=16, pattern="sparse", sync_period=4
    )
    allocations = source.get_allocations()
    placed = OmniAllocator().allocate(allocations)
    _certify(allocations, placed)


@pytest.mark.slow
def test_torture_scalar_scale_is_certified() -> None:
    allocations = _random_scalar(50000, seed=23, horizon=50000)
    placed = OmniAllocator().allocate(allocations)
    _certify(allocations, placed)


@pytest.mark.slow
def test_torture_concurrent_hammer_is_deterministic() -> None:
    source = SyncPatternSource(num_allocations=256, num_threads=8, pattern="sparse")
    allocations = source.get_allocations()
    expected = [a.offset for a in OmniAllocator().allocate(allocations)]
    with ThreadPoolExecutor(max_workers=32) as executor:
        results = list(
            executor.map(lambda _: OmniAllocator().allocate(allocations), range(512))
        )
    assert all([a.offset for a in placed] == expected for placed in results)


@pytest.mark.slow
def test_torture_concurrent_mixed_instances_stay_isolated() -> None:
    instances = tuple(
        SyncPatternSource(
            num_allocations=256, num_threads=4, pattern=pattern, seed=seed
        ).get_allocations()
        for pattern in SYNC_PATTERNS
        for seed in range(16)
    )
    serial_peaks = [_peak(OmniAllocator().allocate(a)) for a in instances]
    serial_pressures = [get_pressure(a, work_budget=None) for a in instances]
    with ThreadPoolExecutor(max_workers=32) as executor:
        parallel_placed = list(
            executor.map(lambda a: OmniAllocator().allocate(a), instances)
        )
        parallel_pressures = list(
            executor.map(lambda a: get_pressure(a, work_budget=None), instances)
        )
    assert [_peak(p) for p in parallel_placed] == serial_peaks
    assert parallel_pressures == serial_pressures
    for allocations, placed in zip(instances, parallel_placed, strict=True):
        _certify(allocations, placed)
