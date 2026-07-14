#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc import run_allocation, validate_allocation
from omnimalloc.benchmark.sources import BaseSource
from omnimalloc.benchmark.sources.concurrent_tiling import ConcurrentTilingSource
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pressure import get_pressure


def _signatures(
    allocations: tuple[Allocation, ...],
) -> list[tuple[object, object, int]]:
    return [(a.start, a.end, a.size) for a in allocations]


def test_concurrent_tiling_is_registered() -> None:
    assert "concurrent_tiling_source" in BaseSource.registry()
    assert BaseSource.get("concurrent_tiling_source") is ConcurrentTilingSource


def test_concurrent_tiling_produces_requested_count_and_dim() -> None:
    source = ConcurrentTilingSource(num_allocations=64, num_threads=4)
    allocations = source.get_allocations()
    assert len(allocations) == 64
    assert {a.dim for a in allocations} == {4}


def test_concurrent_tiling_single_thread_degenerates_to_scalar() -> None:
    source = ConcurrentTilingSource(num_allocations=16, num_threads=1, num_syncs=0)
    assert {a.dim for a in source.get_allocations()} == {1}


def test_concurrent_tiling_unsynced_threads_share_nothing() -> None:
    source = ConcurrentTilingSource(num_allocations=32, num_threads=2, num_syncs=0)
    for alloc in source.get_allocations():
        assert isinstance(alloc.start, tuple)
        assert isinstance(alloc.end, tuple)
        assert sorted(alloc.start).count(0) >= 1
        assert sorted(alloc.end).count(0) >= 1


def test_concurrent_tiling_syncs_propagate_foreign_components() -> None:
    source = ConcurrentTilingSource(num_allocations=64, num_threads=2, num_syncs=64)
    allocations = source.get_allocations()
    assert any(min(a.start) > 0 for a in allocations if isinstance(a.start, tuple))


def test_concurrent_tiling_clocks_are_monotone() -> None:
    source = ConcurrentTilingSource(
        num_allocations=64, num_threads=3, num_syncs=32, capacity=3 * 256 * 1024
    )
    for alloc in source.get_allocations():
        assert all(s <= e for s, e in zip(alloc.start, alloc.end, strict=True))
        assert alloc.start != alloc.end


@pytest.mark.parametrize("num_syncs", [0, 16, 256])
def test_concurrent_tiling_pressure_bounded_by_capacity(num_syncs: int) -> None:
    capacity = 1024 * 1024
    source = ConcurrentTilingSource(
        num_allocations=64, num_threads=4, num_syncs=num_syncs, capacity=capacity
    )
    allocations = source.get_allocations()
    assert max(a.size for a in allocations) <= get_pressure(allocations) <= capacity


def test_concurrent_tiling_ground_truth_is_valid_and_optimal() -> None:
    capacity = 1024 * 1024
    source = ConcurrentTilingSource(
        num_allocations=96, num_threads=4, num_syncs=32, capacity=capacity
    )
    pool = source.get_ground_truth_pool()
    validate_allocation(pool)
    assert pool.is_allocated
    assert pool.size == capacity
    assert 0 < pool.pressure <= capacity
    assert pool.efficiency <= 1.0


def test_concurrent_tiling_ground_truth_matches_get_allocations() -> None:
    source = ConcurrentTilingSource(
        num_allocations=48, num_threads=3, capacity=3 * 256 * 1024
    )
    truth = source.get_ground_truth_pool()
    allocs = source.get_allocations()
    assert _signatures(truth.allocations) == _signatures(allocs)


def test_concurrent_tiling_is_deterministic_per_seed() -> None:
    a = ConcurrentTilingSource(num_allocations=64, seed=7).get_allocations()
    b = ConcurrentTilingSource(num_allocations=64, seed=7).get_allocations()
    c = ConcurrentTilingSource(num_allocations=64, seed=8).get_allocations()
    assert _signatures(a) == _signatures(b)
    assert _signatures(a) != _signatures(c)


def test_concurrent_tiling_distinct_pools_differ() -> None:
    source = ConcurrentTilingSource(num_allocations=32)
    pools = source.get_pools(num_pools=2)
    assert _signatures(pools[0].allocations) != _signatures(pools[1].allocations)


def test_concurrent_tiling_rejects_indivisible_capacity() -> None:
    with pytest.raises(ValueError, match="divisible"):
        ConcurrentTilingSource(num_threads=3, capacity=1024)


def test_concurrent_tiling_rejects_nonpositive_threads() -> None:
    with pytest.raises(ValueError, match="num_threads"):
        ConcurrentTilingSource(num_threads=0)


def test_concurrent_tiling_rejects_fewer_allocations_than_threads() -> None:
    with pytest.raises(ValueError, match="num_threads"):
        ConcurrentTilingSource(
            num_allocations=2, num_threads=4, capacity=4096, min_size=1
        )
    source = ConcurrentTilingSource(
        num_allocations=8, num_threads=4, capacity=4096, min_size=1
    )
    with pytest.raises(ValueError, match="num_threads"):
        source.get_allocations(num_allocations=2)


def test_concurrent_tiling_rejects_band_below_min_size() -> None:
    with pytest.raises(ValueError, match="per-thread capacity"):
        ConcurrentTilingSource(num_threads=4, capacity=4096, min_size=2048)


def test_concurrent_tiling_rejects_num_syncs_out_of_range() -> None:
    with pytest.raises(ValueError, match="num_syncs"):
        ConcurrentTilingSource(num_syncs=-1)
    with pytest.raises(ValueError, match="num_syncs"):
        ConcurrentTilingSource(num_syncs=100, makespan=64, min_size=1, min_duration=1)


def test_concurrent_tiling_no_allocator_beats_the_optimum() -> None:
    capacity = 1024 * 1024
    source = ConcurrentTilingSource(
        num_allocations=96, num_threads=4, num_syncs=64, capacity=capacity
    )
    pool = source.get_pool()
    allocated = run_allocation(pool, "greedy_by_size_allocator_cpp", validate=True)
    assert allocated.size >= capacity
