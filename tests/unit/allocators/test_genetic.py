#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.genetic import HAS_DEAP, GeneticAllocator

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
