#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.greedy import GreedyBySizeAllocator
from omnimalloc.allocators.greedy_base import peak_memory
from omnimalloc.allocators.simulated_annealing import (
    SimulatedAnnealingAllocator,
    SimulatedAnnealingConfig,
)
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.validate import validate_allocation


def _is_valid(result: tuple[Allocation, ...]) -> bool:
    return validate_allocation(Pool(id="test_pool", allocations=result))


def test_simulated_annealing_empty() -> None:
    allocator = SimulatedAnnealingAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


def test_simulated_annealing_single() -> None:
    allocator = SimulatedAnnealingAllocator()
    alloc = Allocation(id=1, size=100, start=0, end=10)
    result = allocator.allocate((alloc,))
    assert len(result) == 1
    assert result[0].offset == 0


def test_simulated_annealing_rejects_non_positive_iterations() -> None:
    with pytest.raises(ValueError, match="max_iterations must be positive"):
        SimulatedAnnealingConfig(max_iterations=0)


def test_simulated_annealing_rejects_negative_temperature() -> None:
    with pytest.raises(ValueError, match="initial_temperature must be non-negative"):
        SimulatedAnnealingConfig(initial_temperature=-1.0)


def test_simulated_annealing_rejects_invalid_cooling_rate() -> None:
    with pytest.raises(ValueError, match="cooling_rate must be in"):
        SimulatedAnnealingConfig(cooling_rate=0.0)
    with pytest.raises(ValueError, match="cooling_rate must be in"):
        SimulatedAnnealingConfig(cooling_rate=1.5)


def test_simulated_annealing_preserves_allocations() -> None:
    allocator = SimulatedAnnealingAllocator()
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=5, end=15),
    )
    result = allocator.allocate(allocs)
    assert len(result) == len(allocs)
    assert {a.id for a in result} == {1, 2}
    assert all(a.offset is not None for a in result)


def test_simulated_annealing_no_temporal_overlap_shares_offset() -> None:
    allocator = SimulatedAnnealingAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=200, start=10, end=20)
    result = allocator.allocate((alloc1, alloc2))
    by_id = {a.id: a for a in result}
    assert by_id[1].offset == 0
    assert by_id[2].offset == 0


def test_simulated_annealing_all_overlap_stacks_sequentially() -> None:
    allocator = SimulatedAnnealingAllocator()
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    result = allocator.allocate(allocs)
    assert _is_valid(result)
    assert peak_memory(result) == 500


def test_simulated_annealing_deterministic() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i % 5 + 1) * 100, start=i % 3, end=i % 3 + i % 7 + 1)
        for i in range(20)
    )
    config = SimulatedAnnealingConfig(max_iterations=200)
    result1 = SimulatedAnnealingAllocator(config).allocate(allocs)
    result2 = SimulatedAnnealingAllocator(config).allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))


def test_simulated_annealing_produces_valid_allocation_on_dense_overlap() -> None:
    config = SimulatedAnnealingConfig(max_iterations=300)
    allocator = SimulatedAnnealingAllocator(config)
    allocs = tuple(
        Allocation(
            id=i,
            size=(i % 7 + 1) * 10,
            start=i % 4,
            end=i % 4 + (i % 3 + 1) * 3,
        )
        for i in range(30)
    )
    result = allocator.allocate(allocs)
    assert _is_valid(result)
    assert {a.id for a in result} == {a.id for a in allocs}


def test_simulated_annealing_matches_or_beats_single_pass_greedy() -> None:
    allocs = tuple(
        Allocation(
            id=i,
            size=(i * 37 % 50 + 1) * 10,
            start=i % 6,
            end=i % 6 + (i * 13 % 5 + 1),
        )
        for i in range(40)
    )
    greedy_peak = peak_memory(GreedyBySizeAllocator().allocate(allocs))
    annealed = SimulatedAnnealingAllocator(
        SimulatedAnnealingConfig(max_iterations=2000)
    ).allocate(allocs)
    assert _is_valid(annealed)
    assert peak_memory(annealed) <= greedy_peak
