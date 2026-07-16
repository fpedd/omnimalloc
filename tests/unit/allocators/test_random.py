#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.allocators.greedy_base import peak_memory
from omnimalloc.allocators.random import RandomAllocator
from omnimalloc.analysis.pressure import get_pressure
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool
from omnimalloc.validate import validate_allocation


def _allocs(count: int) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(id=i, size=(i % 5 + 1) * 10, start=i % 3, end=i % 3 + i % 4 + 1)
        for i in range(count)
    )


def test_random_empty() -> None:
    result = RandomAllocator().allocate(())
    assert len(result) == 0


def test_random_single() -> None:
    result = RandomAllocator().allocate((Allocation(id=1, size=100, start=0, end=10),))
    assert len(result) == 1
    assert result[0].offset == 0


def test_random_zero_trials_falls_back_to_insertion_order_greedy() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=5, end=15),
    )
    result = RandomAllocator(num_trials=0).allocate(allocs)
    assert [a.offset for a in result] == [0, 100]


def test_random_produces_valid_allocation() -> None:
    allocs = _allocs(20)
    result = RandomAllocator(num_trials=20).allocate(allocs)
    assert validate_allocation(Pool(id="test_pool", allocations=result))
    assert {a.id for a in result} == {a.id for a in allocs}


def test_random_deterministic_for_same_seed() -> None:
    allocs = _allocs(20)
    result1 = RandomAllocator(num_trials=10, seed=7).allocate(allocs)
    result2 = RandomAllocator(num_trials=10, seed=7).allocate(allocs)
    assert {a.id: a.offset for a in result1} == {a.id: a.offset for a in result2}


def test_random_repeated_calls_on_same_instance_are_deterministic() -> None:
    allocs = _allocs(20)
    allocator = RandomAllocator(num_trials=10, seed=7)
    first = allocator.allocate(allocs)
    second = allocator.allocate(allocs)
    assert {a.id: a.offset for a in first} == {a.id: a.offset for a in second}


def test_random_more_trials_never_worse_for_same_seed() -> None:
    allocs = _allocs(30)
    few = RandomAllocator(num_trials=5, seed=3).allocate(allocs)
    many = RandomAllocator(num_trials=50, seed=3).allocate(allocs)
    assert peak_memory(many) <= peak_memory(few)


def test_random_peak_within_problem_bounds() -> None:
    allocs = _allocs(30)
    result = RandomAllocator(num_trials=30, seed=1).allocate(allocs)
    assert validate_allocation(Pool(id="test_pool", allocations=result))
    assert get_pressure(allocs) <= peak_memory(result) <= sum(a.size for a in allocs)
