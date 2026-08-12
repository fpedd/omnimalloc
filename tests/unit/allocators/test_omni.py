#
# SPDX-License-Identifier: Apache-2.0
#

import random

import pytest
from omnimalloc.allocators import (
    BaseAllocator,
    GreedyByAllAllocator,
    NaiveAllocator,
    OmniAllocator,
)
from omnimalloc.analysis import antichain_pressure, placement_pressure
from omnimalloc.benchmark.sources.concurrent_tiling import ConcurrentTilingSource
from omnimalloc.benchmark.sources.sync_patterns import SYNC_PATTERNS, SyncPatternSource
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.validate import validate_allocation


def _random_scalar(n: int, seed: int) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(n):
        start = rng.randint(0, 50)
        allocations.append(
            Allocation(
                id=i,
                size=rng.randint(1, 128),
                start=start,
                end=start + rng.randint(1, 20),
            )
        )
    return tuple(allocations)


def _random_vector(n: int, dim: int, seed: int) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(n):
        start = tuple(rng.randint(0, 6) for _ in range(dim))
        delta = [rng.randint(0, 2) for _ in range(dim)]
        if sum(delta) == 0:
            delta[rng.randrange(dim)] = 1
        allocations.append(
            Allocation(
                id=i,
                size=rng.randint(1, 64),
                start=start,
                end=tuple(s + d for s, d in zip(start, delta, strict=True)),
            )
        )
    return tuple(allocations)


def _two_plus_two() -> tuple[Allocation, ...]:
    return (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )


def _best_greedy_peak(allocations: tuple[Allocation, ...]) -> int:
    return placement_pressure(GreedyByAllAllocator(num_threads=1).allocate(allocations))


def test_omni_is_registered_and_supports_vector_time() -> None:
    assert BaseAllocator.get("omni") is OmniAllocator
    assert OmniAllocator.supports_vector_time is True


def test_omni_rejects_negative_linearize_budget() -> None:
    with pytest.raises(ValueError, match="linearize_budget"):
        OmniAllocator(linearize_budget=-1)


def test_omni_empty_returns_empty() -> None:
    assert OmniAllocator().allocate(()) == ()


def test_omni_single_allocation_at_offset_zero() -> None:
    placed = OmniAllocator().allocate((Allocation(id=1, size=64, start=0, end=4),))
    assert placed[0].offset == 0


def test_omni_scalar_placement_is_valid_and_bounded() -> None:
    allocations = _random_scalar(200, seed=1)
    placed = OmniAllocator().allocate(allocations)
    validate_allocation(Pool(id="p", allocations=placed))
    assert antichain_pressure(allocations) <= placement_pressure(placed)
    assert placement_pressure(placed) <= sum(a.size for a in allocations)


def test_omni_scalar_not_worse_than_naive() -> None:
    allocations = _random_scalar(150, seed=2)
    omni = OmniAllocator().allocate(allocations)
    naive = NaiveAllocator().allocate(allocations)
    assert placement_pressure(omni) <= placement_pressure(naive)


def test_omni_preserves_vector_times_and_metadata() -> None:
    allocations = _two_plus_two()
    placed = OmniAllocator().allocate(allocations)
    assert [(a.id, a.size, a.start, a.end) for a in placed] == [
        (a.id, a.size, a.start, a.end) for a in allocations
    ]
    assert all(a.offset is not None for a in placed)


def test_omni_non_linearizable_placement_is_valid() -> None:
    placed = OmniAllocator().allocate(_two_plus_two())
    validate_allocation(Pool(id="p", allocations=placed))
    assert placement_pressure(placed) >= 64 + 16


def test_omni_lockstep_matches_scalar_peak() -> None:
    scalar = _random_scalar(100, seed=3)
    lockstep = tuple(
        Allocation(id=a.id, size=a.size, start=(a.start, a.start), end=(a.end, a.end))
        for a in scalar
    )
    lockstep_peak = placement_pressure(OmniAllocator().allocate(lockstep))
    assert lockstep_peak == placement_pressure(OmniAllocator().allocate(scalar))


def test_omni_is_deterministic() -> None:
    source = SyncPatternSource(num_allocations=64, num_threads=4, pattern="groups")
    allocations = source.get_allocations()
    first = OmniAllocator().allocate(allocations)
    second = OmniAllocator().allocate(allocations)
    assert [a.offset for a in first] == [a.offset for a in second]


def test_omni_keeps_existing_offsets_pinned() -> None:
    allocations = tuple(
        Allocation(id=i, size=32, start=0, end=4, offset=1024 * (i + 1))
        for i in range(4)
    )
    placed = OmniAllocator().allocate(allocations)
    assert [a.offset for a in placed] == [1024, 2048, 3072, 4096]


def test_omni_packs_free_allocations_around_pins() -> None:
    allocations = (
        Allocation(id="pinned", size=32, start=0, end=4, offset=64),
        *(Allocation(id=i, size=32, start=0, end=4) for i in range(3)),
    )
    placed = OmniAllocator().allocate(allocations)
    offsets = {a.id: a.offset for a in placed}
    assert offsets["pinned"] == 64
    assert sorted(offsets[i] for i in range(3)) == [0, 32, 96]
    assert placement_pressure(placed) == 128


def test_omni_handles_extreme_durations() -> None:
    allocations = tuple(
        Allocation(id=i, size=16 + i, start=0, end=10**18) for i in range(4)
    )
    placed = OmniAllocator().allocate(allocations)
    validate_allocation(Pool(id="p", allocations=placed))
    assert placement_pressure(placed) == sum(a.size for a in allocations)


def test_omni_rejects_duplicate_ids() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=1, size=8, start=0, end=4),
    )
    with pytest.raises(ValueError, match="unique"):
        OmniAllocator().allocate(allocations)


def test_omni_rejects_mixed_dimensions() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=(0, 1), end=(2, 2)),
    )
    with pytest.raises(ValueError, match="dimension"):
        OmniAllocator().allocate(allocations)


def test_omni_matches_greedy_portfolio_on_scalar_input() -> None:
    allocations = _random_scalar(120, seed=4)
    omni = placement_pressure(OmniAllocator().allocate(allocations))
    assert omni == _best_greedy_peak(allocations)


def test_omni_without_linearization_matches_greedy_portfolio() -> None:
    allocations = _random_vector(30, dim=3, seed=5)
    omni = placement_pressure(OmniAllocator(linearize_budget=0).allocate(allocations))
    assert omni == _best_greedy_peak(allocations)


def test_omni_linearize_budget_does_not_lower_quality() -> None:
    allocations = (
        Allocation(id=0, size=30, start=(0, 2), end=(0, 3)),
        Allocation(id=1, size=2, start=(0, 2), end=(0, 3)),
        Allocation(id=2, size=19, start=(0, 1), end=(2, 1)),
        Allocation(id=3, size=6, start=(4, 0), end=(6, 2)),
        Allocation(id=4, size=23, start=(4, 4), end=(5, 6)),
        Allocation(id=5, size=9, start=(1, 1), end=(1, 3)),
        Allocation(id=6, size=24, start=(0, 3), end=(2, 3)),
        Allocation(id=7, size=11, start=(2, 3), end=(3, 3)),
        Allocation(id=8, size=26, start=(4, 0), end=(4, 1)),
        Allocation(id=9, size=26, start=(2, 4), end=(4, 6)),
        Allocation(id=10, size=29, start=(1, 4), end=(3, 5)),
    )
    unlinearized = OmniAllocator(linearize_budget=0).allocate(allocations)
    linearized = OmniAllocator(linearize_budget=None).allocate(allocations)
    assert placement_pressure(unlinearized) == _best_greedy_peak(allocations) == 104
    assert placement_pressure(linearized) == 104


def test_omni_linearization_widens_the_portfolio() -> None:
    allocations = (
        Allocation(id=0, size=1, start=(6, 12), end=(9, 15)),
        Allocation(id=1, size=2, start=(16, 7), end=(24, 11)),
        Allocation(id=2, size=16, start=(17, 8), end=(20, 14)),
        Allocation(id=3, size=16, start=(8, 2), end=(14, 10)),
        Allocation(id=4, size=4, start=(0, 4), end=(7, 12)),
        Allocation(id=5, size=32, start=(12, 19), end=(13, 25)),
        Allocation(id=6, size=2, start=(5, 9), end=(12, 17)),
        Allocation(id=7, size=8, start=(9, 17), end=(14, 23)),
        Allocation(id=8, size=1, start=(16, 4), end=(22, 9)),
        Allocation(id=9, size=16, start=(6, 12), end=(9, 19)),
        Allocation(id=10, size=8, start=(11, 20), end=(15, 24)),
        Allocation(id=11, size=8, start=(17, 9), end=(20, 10)),
        Allocation(id=12, size=16, start=(19, 9), end=(25, 12)),
        Allocation(id=13, size=32, start=(19, 7), end=(24, 11)),
        Allocation(id=14, size=16, start=(3, 7), end=(9, 9)),
    )
    widened = OmniAllocator().allocate(allocations)
    base_only = OmniAllocator(linearize_budget=0).allocate(allocations)
    assert placement_pressure(base_only) == _best_greedy_peak(allocations) == 140
    assert placement_pressure(widened) == 139


@pytest.mark.parametrize("num_syncs", [0, 16, 256])
def test_omni_concurrent_tiling_stays_near_optimum(num_syncs: int) -> None:
    capacity = 1024 * 1024
    source = ConcurrentTilingSource(
        num_allocations=96, num_threads=4, num_syncs=num_syncs, capacity=capacity
    )
    placed = OmniAllocator().allocate(source.get_allocations())
    validate_allocation(Pool(id="p", allocations=placed))
    assert capacity <= placement_pressure(placed) <= 2 * capacity


@pytest.mark.parametrize("pattern", SYNC_PATTERNS)
def test_omni_torture_across_sync_patterns(pattern: str) -> None:
    for seed in (0, 1, 2):
        source = SyncPatternSource(
            num_allocations=48, num_threads=4, pattern=pattern, seed=seed
        )
        allocations = source.get_allocations()
        placed = OmniAllocator().allocate(allocations)
        validate_allocation(Pool(id=f"{pattern}-{seed}", allocations=placed))
        naive = NaiveAllocator().allocate(allocations)
        assert placement_pressure(placed) <= placement_pressure(naive)


def test_omni_torture_across_tiling_variants() -> None:
    for seed in (0, 1, 2):
        for num_syncs in (0, 32):
            source = ConcurrentTilingSource(
                num_allocations=64, num_threads=2, num_syncs=num_syncs, seed=seed
            )
            placed = OmniAllocator().allocate(source.get_allocations())
            validate_allocation(Pool(id=f"{num_syncs}-{seed}", allocations=placed))


def test_omni_linearize_budget_is_quality_monotone() -> None:
    for seed in range(10):
        for dim in (2, 3, 4):
            allocations = _random_vector(24, dim=dim, seed=1000 * dim + seed)
            default = OmniAllocator().allocate(allocations)
            unbounded = OmniAllocator(linearize_budget=None).allocate(allocations)
            floor = _best_greedy_peak(allocations)
            assert placement_pressure(default) <= floor
            assert placement_pressure(unbounded) <= floor
            validate_allocation(Pool(id=f"{dim}-{seed}", allocations=default))


def test_omni_degenerate_clock_columns_match_the_scalar_instance() -> None:
    rng = random.Random(31)
    for _ in range(60):
        base = [
            (i, rng.randint(1, 50), rng.randint(0, 60), rng.randint(1, 12))
            for i in range(rng.randint(1, 40))
        ]
        scalar = tuple(
            Allocation(id=i, size=z, start=s, end=s + d) for i, z, s, d in base
        )
        padded = tuple(
            Allocation(id=i, size=z, start=(s, 0, 0), end=(s + d, 0, 0))
            for i, z, s, d in base
        )
        lockstep = tuple(
            Allocation(id=i, size=z, start=(s,) * 3, end=(s + d,) * 3)
            for i, z, s, d in base
        )
        expected = placement_pressure(OmniAllocator().allocate(scalar))
        for allocations in (padded, lockstep):
            placed = OmniAllocator().allocate(allocations)
            validate_allocation(Pool(id="p", allocations=placed))
            assert placement_pressure(placed) == expected
