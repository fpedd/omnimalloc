#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import replace

import pytest
from omnimalloc import allocate, validate_allocation
from omnimalloc.analysis import antichain_pressure
from omnimalloc.benchmark.sources import BaseSource
from omnimalloc.benchmark.sources.tiling import TilingSource
from omnimalloc.primitives import Allocation


def _signatures(allocations: tuple[Allocation, ...]) -> list[tuple[int, int, int]]:
    return [(a.start, a.end, a.size) for a in allocations]


def test_tiling_source_is_registered() -> None:
    assert "tiling" in BaseSource.registry()
    assert BaseSource.get("tiling") is TilingSource


def test_tiling_source_produces_requested_count() -> None:
    source = TilingSource(num_allocations=128)
    allocations = source.get_allocations()
    assert len(allocations) == 128


@pytest.mark.parametrize("num", [1, 16, 64, 256, 512])
def test_tiling_optimum_is_tight(num: int) -> None:
    capacity = 1024 * 1024
    source = TilingSource(num_allocations=num, capacity=capacity)
    allocations = source.get_allocations()
    assert antichain_pressure(allocations) == capacity


def test_tiling_allocations_fit_within_makespan() -> None:
    makespan = 4096
    source = TilingSource(num_allocations=64, makespan=makespan, size_min=1)
    for alloc in source.get_allocations():
        assert 0 <= alloc.start < alloc.end <= makespan


def test_tiling_respects_min_size() -> None:
    source = TilingSource(num_allocations=256, size_min=2048)
    assert all(a.size >= 2048 for a in source.get_allocations())


def test_tiling_zero_requested_returns_empty() -> None:
    assert TilingSource().get_allocations(num_allocations=0) == ()


def test_tiling_is_deterministic_per_seed() -> None:
    a = TilingSource(num_allocations=128, seed=7).get_allocations()
    b = TilingSource(num_allocations=128, seed=7).get_allocations()
    c = TilingSource(num_allocations=128, seed=8).get_allocations()
    assert _signatures(a) == _signatures(b)
    assert _signatures(a) != _signatures(c)


def test_tiling_distinct_pools_differ() -> None:
    source = TilingSource(num_allocations=32)
    pools = source.get_pools(num_pools=2)
    assert len(pools) == 2
    assert _signatures(pools[0].allocations) != _signatures(pools[1].allocations)


def test_tiling_rejects_invalid_mem_cut_prob() -> None:
    with pytest.raises(ValueError, match="mem_cut_prob"):
        TilingSource(mem_cut_prob=1.5)


def test_tiling_rejects_capacity_below_min_size() -> None:
    with pytest.raises(ValueError, match="capacity"):
        TilingSource(capacity=10, size_min=1024)


def test_tiling_rejects_nonpositive_min_size() -> None:
    with pytest.raises(ValueError, match="size_min"):
        TilingSource(size_min=0)


def test_tiling_raises_when_count_unreachable() -> None:
    source = TilingSource(
        num_allocations=100, capacity=1024, size_min=1024, makespan=10, duration_min=5
    )
    with pytest.raises(ValueError, match="cannot reach"):
        source.get_allocations()


def test_tiling_ground_truth_is_valid_and_optimal() -> None:
    capacity = 1024 * 1024
    source = TilingSource(num_allocations=200, capacity=capacity)
    pool = source.get_ground_truth_pool()

    validate_allocation(pool)
    assert pool.is_allocated
    assert pool.size == capacity
    assert pool.pressure == capacity


def test_tiling_ground_truth_matches_get_allocations() -> None:
    source = TilingSource(num_allocations=64)
    truth = source.get_ground_truth_pool()
    allocs = source.get_allocations()
    assert _signatures(truth.allocations) == _signatures(allocs)


def test_tiling_ground_truth_available_per_pool() -> None:
    source = TilingSource(num_allocations=32)
    pools = source.get_pools(num_pools=2)
    truth = source.get_ground_truth_pool(skip=32)
    assert _signatures(truth.allocations) == _signatures(pools[1].allocations)


def test_tiling_ground_truth_requires_seed() -> None:
    with pytest.raises(ValueError, match="seed"):
        TilingSource(seed=None).get_ground_truth_pool()


def test_tiling_known_optimum_is_the_capacity() -> None:
    capacity = 1024 * 1024
    source = TilingSource(num_allocations=64, capacity=capacity)
    assert source.get_known_optimum() == capacity
    assert source.get_known_optimum(128) == capacity


def test_tiling_known_optimum_unknown_without_seed() -> None:
    assert TilingSource(num_allocations=64, seed=None).get_known_optimum() is None


def test_tiling_variant_sweep_builds_ladder() -> None:
    source = TilingSource(capacity=1024 * 1024)
    for num in (64, 128, 256):
        pool = source.get_variant(num)
        assert len(pool.allocations) == num
        assert pool.pressure == 1024 * 1024


def test_tiling_no_allocator_beats_the_optimum() -> None:
    capacity = 1024 * 1024
    source = TilingSource(num_allocations=150, capacity=capacity)
    pool = source.get_pool()
    allocated = allocate(pool, "greedy_by_size", validate=True)
    assert allocated.size >= capacity


def test_memory_declares_the_achievable_capacity() -> None:
    source = TilingSource(num_allocations=64, capacity=4096, makespan=1024)
    memory = source.get_memory()
    assert memory.size == 4096


def test_memory_capacity_scales_with_the_pool_count() -> None:
    source = TilingSource(num_allocations=32, capacity=4096, makespan=1024)
    source.num_pools = 3
    assert source.get_memory().size == 3 * 4096


def test_capacity_constrained_memory_validates_when_placed_well() -> None:
    source = TilingSource(num_allocations=32, capacity=4096, makespan=1024)
    validate_allocation(allocate(source.get_memory(), "omni"), require_capacity=True)


def test_an_undersized_memory_is_rejected() -> None:
    source = TilingSource(num_allocations=32, capacity=4096, makespan=1024)
    memory = replace(source.get_memory(), size=1024)
    with pytest.raises(ValueError, match="exceeds memory size"):
        validate_allocation(allocate(memory, "omni"))
