#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import replace

import pytest
from omnimalloc import allocate, validate_allocation
from omnimalloc.analysis import antichain_pressure
from omnimalloc.benchmark.sources.tiling import TilingSource
from omnimalloc.primitives import Allocation


def _signatures(allocations: tuple[Allocation, ...]) -> list[tuple[int, int, int]]:
    return [(a.start, a.end, a.size) for a in allocations]


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


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        pytest.param({"mem_cut_prob": 1.5}, "mem_cut_prob", id="invalid_mem_cut_prob"),
        pytest.param(
            {"capacity": 10, "size_min": 1024}, "capacity", id="capacity_below_min_size"
        ),
        pytest.param({"size_min": 0}, "size_min", id="nonpositive_min_size"),
    ],
)
def test_tiling_rejects(kwargs: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        TilingSource(**kwargs)


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


@pytest.mark.parametrize("seed", [0, None])
def test_tiling_known_optimum_is_the_capacity(seed: int | None) -> None:
    assert TilingSource(capacity=4096, seed=seed).get_known_optimum() == 4096


def test_tiling_variant_sweep_builds_ladder() -> None:
    source = TilingSource(capacity=1024 * 1024)
    for num in (64, 128, 256):
        pool = source.get_variant(num)
        assert len(pool.allocations) == num
        assert pool.pressure == 1024 * 1024


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
