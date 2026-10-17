#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc import Allocation, allocate
from omnimalloc.allocators.genetic import HAS_DEAP, GeneticAllocator
from omnimalloc.analysis import placement_pressure
from omnimalloc.benchmark.sources import HighContentionSource

pytestmark = pytest.mark.skipif(not HAS_DEAP, reason="deap not installed")


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"population_size": 0}, "population_size must be positive"),
        ({"max_generations": -1}, "max_generations must be non-negative"),
        ({"crossover_prob": 1.5}, "must be in"),
        ({"mutation_prob": -0.1}, "must be in"),
        ({"tournament_size": 0}, "tournament_size must be positive"),
    ],
)
def test_genetic_rejects(kwargs: dict[str, float], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        GeneticAllocator(**kwargs)


def test_genetic_seeds_contention_on_linearized_clocks() -> None:
    scalar = HighContentionSource(num_allocations=40, seed=6).get_allocations()
    vector = tuple(
        Allocation(
            id=a.id, size=a.size, start=(a.start, 2 * a.start), end=(a.end, 2 * a.end)
        )
        for a in scalar
    )
    seeded = GeneticAllocator(population_size=7, max_generations=0).allocate(vector)
    contention = allocate(vector, "greedy_by_contention")
    assert placement_pressure(seeded) <= placement_pressure(contention)
