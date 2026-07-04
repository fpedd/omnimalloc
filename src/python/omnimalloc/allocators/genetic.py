#
# SPDX-License-Identifier: Apache-2.0
#

import random
from typing import Any, cast

from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.common.optional import require_optional
from omnimalloc.primitives import Allocation

from .base import require_unique_ids
from .greedy import GreedyAllocator
from .greedy_base import (
    order_by_area,
    order_by_conflict,
    order_by_conflict_size,
    order_by_duration,
    order_by_size,
    order_by_start,
)

try:
    from deap import algorithms, base, creator, tools

    HAS_DEAP = True
except ImportError:
    HAS_DEAP = False
    algorithms = base = creator = tools = cast("Any", None)


class GeneticAllocator(GreedyAllocator):
    """Genetic algorithm allocator that evolves greedy placement orders."""

    def __init__(
        self,
        seed: int = 42,
        population_size: int = 100,
        num_generations: int = 50,
        crossover_prob: float = 0.7,
        mutation_prob: float = 0.2,
        tournament_size: int = 3,
    ) -> None:
        if not HAS_DEAP:
            require_optional("deap", "GeneticAllocator")
        if population_size <= 0:
            raise ValueError(f"population_size must be positive, got {population_size}")
        if num_generations < 0:
            raise ValueError(
                f"num_generations must be non-negative, got {num_generations}"
            )
        if not 0.0 <= crossover_prob <= 1.0 or not 0.0 <= mutation_prob <= 1.0:
            raise ValueError(
                f"crossover_prob and mutation_prob must be in [0, 1], "
                f"got {crossover_prob} and {mutation_prob}"
            )
        if tournament_size <= 0:
            raise ValueError(f"tournament_size must be positive, got {tournament_size}")

        self.seed = seed
        self.population_size = population_size
        self.num_generations = num_generations
        self.crossover_prob = crossover_prob
        self.mutation_prob = mutation_prob
        self.tournament_size = tournament_size

        # Setup DEAP creators (only once per process, they live in a global namespace)
        if not hasattr(creator, "FitnessMin"):
            creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
        if not hasattr(creator, "Individual"):
            # FitnessMin is dynamically created by DEAP
            creator.create("Individual", list, fitness=creator.FitnessMin)  # type: ignore[possibly-missing-attribute]

    def _evaluate_permutation(
        self, permutation: list[int], placer: FirstFitPlacer
    ) -> tuple[float]:
        """Evaluate a permutation by computing its greedy peak memory usage."""
        return (float(placer.evaluate(permutation)),)

    def _heuristic_permutations(
        self, allocations: tuple[Allocation, ...]
    ) -> list[list[int]]:
        """Create seed permutations mirroring the greedy sort heuristics."""
        orders = (
            order_by_size,
            order_by_duration,
            order_by_area,
            order_by_conflict,
            order_by_conflict_size,
            order_by_start,
        )
        positions = {alloc.id: i for i, alloc in enumerate(allocations)}
        permutations = [
            [positions[alloc.id] for alloc in order(allocations)] for order in orders
        ]
        return permutations[: self.population_size]

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        """Evolve permutations using a genetic algorithm to find best allocation."""
        if len(allocations) < 2:
            return super().allocate(allocations)

        require_unique_ids(allocations)

        # DEAP operators draw from the global random module
        random.seed(self.seed)

        placer = FirstFitPlacer(list(allocations))
        toolbox = base.Toolbox()
        n = len(allocations)
        toolbox.register("indices", random.sample, range(n), n)
        # Individual and indices are dynamically created by DEAP
        toolbox.register(
            "individual",
            tools.initIterate,
            creator.Individual,  # type: ignore[possibly-missing-attribute]
            toolbox.indices,  # type: ignore[possibly-missing-attribute]
        )
        toolbox.register("evaluate", self._evaluate_permutation, placer=placer)
        toolbox.register("mate", tools.cxOrdered)
        toolbox.register("mutate", tools.mutShuffleIndexes, indpb=0.05)
        # TODO(fpedd): Try larger tournsize and selNSGA2
        toolbox.register("select", tools.selTournament, tournsize=self.tournament_size)

        # Seed the population with heuristic orders, fill up with random ones
        # Individual and individual() are dynamically created by DEAP
        population = [
            creator.Individual(permutation)  # type: ignore[possibly-missing-attribute]
            for permutation in self._heuristic_permutations(allocations)
        ]
        population += [
            toolbox.individual()  # type: ignore[possibly-missing-attribute]
            for _ in range(self.population_size - len(population))
        ]

        hall_of_fame = tools.HallOfFame(maxsize=1)

        # TODO(fpedd): Try eaMuPlusLambda and eaMuCommaLambda
        algorithms.eaSimple(
            population=population,
            toolbox=toolbox,
            cxpb=self.crossover_prob,
            mutpb=self.mutation_prob,
            ngen=self.num_generations,
            halloffame=hall_of_fame,
            verbose=False,
        )

        best_permutation = list(hall_of_fame[0])
        return tuple(placer.place(best_permutation))
