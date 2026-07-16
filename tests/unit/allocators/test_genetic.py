#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.genetic import HAS_DEAP, GeneticAllocator
from omnimalloc.allocators.greedy import GreedyBySizeAllocator
from omnimalloc.allocators.greedy_base import peak_memory
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool
from omnimalloc.validate import validate_allocation

pytestmark = pytest.mark.skipif(not HAS_DEAP, reason="deap not installed")


def _fast_allocator(seed: int = 42) -> GeneticAllocator:
    return GeneticAllocator(seed=seed, population_size=20, num_generations=5)


def _allocs(count: int) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(id=i, size=(i % 5 + 1) * 10, start=i % 3, end=i % 3 + i % 4 + 1)
        for i in range(count)
    )


def test_genetic_empty() -> None:
    result = _fast_allocator().allocate(())
    assert len(result) == 0


def test_genetic_single() -> None:
    result = _fast_allocator().allocate((Allocation(id=1, size=100, start=0, end=10),))
    assert len(result) == 1
    assert result[0].offset == 0


def test_genetic_rejects_invalid_population_size() -> None:
    with pytest.raises(ValueError, match="population_size must be positive"):
        GeneticAllocator(population_size=0)


def test_genetic_rejects_negative_generations() -> None:
    with pytest.raises(ValueError, match="num_generations must be non-negative"):
        GeneticAllocator(num_generations=-1)


def test_genetic_rejects_out_of_range_probabilities() -> None:
    with pytest.raises(ValueError, match="must be in"):
        GeneticAllocator(crossover_prob=1.5)
    with pytest.raises(ValueError, match="must be in"):
        GeneticAllocator(mutation_prob=-0.1)


def test_genetic_rejects_invalid_tournament_size() -> None:
    with pytest.raises(ValueError, match="tournament_size must be positive"):
        GeneticAllocator(tournament_size=0)


def test_genetic_rejects_negative_timeout() -> None:
    with pytest.raises(ValueError, match="timeout must be positive or None"):
        GeneticAllocator(timeout=-1.0)


def test_genetic_rejects_duplicate_ids() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10)
    with pytest.raises(ValueError, match="allocation ids must be unique"):
        _fast_allocator().allocate((alloc, alloc))


def test_genetic_produces_valid_allocation() -> None:
    allocs = _allocs(20)
    result = _fast_allocator().allocate(allocs)
    assert validate_allocation(Pool(id="test_pool", allocations=result))
    assert {a.id for a in result} == {a.id for a in allocs}
    assert all(a.offset is not None for a in result)


def test_genetic_deterministic_for_same_seed() -> None:
    allocs = _allocs(20)
    result1 = _fast_allocator(seed=7).allocate(allocs)
    result2 = _fast_allocator(seed=7).allocate(allocs)
    assert {a.id: a.offset for a in result1} == {a.id: a.offset for a in result2}


def test_genetic_never_worse_than_greedy_by_size_seed() -> None:
    allocs = _allocs(30)
    baseline = peak_memory(GreedyBySizeAllocator().allocate(allocs))
    result = _fast_allocator().allocate(allocs)
    assert peak_memory(result) <= baseline


def test_genetic_improves_or_matches_adversarial_insertion_order() -> None:
    allocs = tuple(
        Allocation(
            id=i,
            size=10 if i < 15 else 1000,
            start=(i * 2) % 10,
            end=(i * 2) % 10 + 3,
        )
        for i in range(30)
    )
    result = _fast_allocator().allocate(allocs)
    assert validate_allocation(Pool(id="test_pool", allocations=result))
    baseline = peak_memory(GreedyBySizeAllocator().allocate(allocs))
    assert peak_memory(result) <= baseline


def test_genetic_preserves_global_random_state() -> None:
    import random

    random.seed(7)
    expected = [random.random() for _ in range(3)]
    random.seed(7)
    _fast_allocator().allocate(_allocs(10))
    assert [random.random() for _ in range(3)] == expected
