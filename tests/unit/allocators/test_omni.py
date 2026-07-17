#
# SPDX-License-Identifier: Apache-2.0
#

import random

import pytest
from omnimalloc._cpp import OmniAllocatorCpp
from omnimalloc.allocators import BaseAllocator, NaiveAllocator, OmniAllocator
from omnimalloc.analysis.pressure import get_placement_pressure, get_pressure
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


def _two_plus_two() -> tuple[Allocation, ...]:
    return (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )


def test_omni_is_registered_and_supports_vector_time() -> None:
    assert BaseAllocator.get("omni_allocator") is OmniAllocator
    assert OmniAllocator.supports_vector_time is True


def test_omni_rejects_negative_linearize_budget() -> None:
    with pytest.raises(ValueError, match="linearize_budget"):
        OmniAllocator(linearize_budget=-1)


def test_omni_cpp_repr_includes_budget() -> None:
    assert repr(OmniAllocatorCpp(100)) == "OmniAllocator(linearize_budget=100)"
    assert repr(OmniAllocatorCpp(None)) == "OmniAllocator(linearize_budget=None)"


def test_omni_empty_returns_empty() -> None:
    assert OmniAllocator().allocate(()) == ()


def test_omni_single_allocation_at_offset_zero() -> None:
    placed = OmniAllocator().allocate((Allocation(id=1, size=64, start=0, end=4),))
    assert placed[0].offset == 0


def test_omni_scalar_placement_is_valid_and_bounded() -> None:
    allocations = _random_scalar(200, seed=1)
    placed = OmniAllocator().allocate(allocations)
    validate_allocation(Pool(id="p", allocations=placed))
    assert get_pressure(allocations) <= get_placement_pressure(placed)
    assert get_placement_pressure(placed) <= sum(a.size for a in allocations)


def test_omni_scalar_not_worse_than_naive() -> None:
    allocations = _random_scalar(150, seed=2)
    omni = OmniAllocator().allocate(allocations)
    naive = NaiveAllocator().allocate(allocations)
    assert get_placement_pressure(omni) <= get_placement_pressure(naive)


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
    assert get_placement_pressure(placed) >= 64 + 16


def test_omni_lockstep_matches_scalar_peak() -> None:
    scalar = _random_scalar(100, seed=3)
    lockstep = tuple(
        Allocation(id=a.id, size=a.size, start=(a.start, a.start), end=(a.end, a.end))
        for a in scalar
    )
    lockstep_peak = get_placement_pressure(OmniAllocator().allocate(lockstep))
    assert lockstep_peak == get_placement_pressure(OmniAllocator().allocate(scalar))


def test_omni_is_deterministic() -> None:
    source = SyncPatternSource(num_allocations=64, num_threads=4, pattern="groups")
    allocations = source.get_allocations()
    first = OmniAllocator().allocate(allocations)
    second = OmniAllocator().allocate(allocations)
    assert [a.offset for a in first] == [a.offset for a in second]


def test_omni_ignores_existing_offsets() -> None:
    allocations = tuple(
        Allocation(id=i, size=32, start=0, end=4, offset=1024 * (i + 1))
        for i in range(4)
    )
    placed = OmniAllocator().allocate(allocations)
    assert get_placement_pressure(placed) == 128


def test_omni_handles_extreme_durations() -> None:
    allocations = tuple(
        Allocation(id=i, size=16 + i, start=0, end=10**18) for i in range(4)
    )
    placed = OmniAllocator().allocate(allocations)
    validate_allocation(Pool(id="p", allocations=placed))
    assert get_placement_pressure(placed) == sum(a.size for a in allocations)


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


@pytest.mark.parametrize("num_syncs", [0, 16, 256])
def test_omni_concurrent_tiling_stays_near_optimum(num_syncs: int) -> None:
    capacity = 1024 * 1024
    source = ConcurrentTilingSource(
        num_allocations=96, num_threads=4, num_syncs=num_syncs, capacity=capacity
    )
    placed = OmniAllocator().allocate(source.get_allocations())
    validate_allocation(Pool(id="p", allocations=placed))
    assert capacity <= get_placement_pressure(placed) <= 2 * capacity


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
        assert get_placement_pressure(placed) <= get_placement_pressure(naive)


def test_omni_torture_across_tiling_variants() -> None:
    for seed in (0, 1, 2):
        for num_syncs in (0, 32):
            source = ConcurrentTilingSource(
                num_allocations=64, num_threads=2, num_syncs=num_syncs, seed=seed
            )
            placed = OmniAllocator().allocate(source.get_allocations())
            validate_allocation(Pool(id=f"{num_syncs}-{seed}", allocations=placed))
