#
# SPDX-License-Identifier: Apache-2.0
#

import random

import numpy as np

from omnimalloc.common.optional import require_optional
from omnimalloc.primitives import Allocation

from .greedy import GreedyAllocator

try:
    from deap import algorithms, base, creator, tools

    HAS_DEAP = True

except ImportError:
    from types import SimpleNamespace
    from typing import Any

    HAS_DEAP = False

    algorithms: Any = SimpleNamespace(
        eaSimple=None,
    )
    base: Any = SimpleNamespace(
        Fitness=None,
        Toolbox=None,
    )
    creator: Any = SimpleNamespace(
        create=None,
    )
    tools: Any = SimpleNamespace(
        initIterate=None,
        selTournament=None,
        cxOrdered=None,
        mutShuffleIndexes=None,
    )


class GeneticAllocator(GreedyAllocator):
    """Genetic algorithm allocator that evolves permutation orders."""

    def __init__(
        self,
        seed: int = 42,
        population_size: int = 100,
        num_generations: int = 50,
        crossover_prob: float = 0.7,
        mutation_prob: float = 0.2,
        tournament_size: int = 3,
    ) -> None:
        """Initialize the genetic allocator."""
        if not HAS_DEAP:
            require_optional("deap", "GeneticAllocator")

        self.seed = seed
        self.population_size = population_size
        self.num_generations = num_generations
        self.crossover_prob = crossover_prob
        self.mutation_prob = mutation_prob
        self.tournament_size = tournament_size

        # Setup DEAP creators (only once per class)
        if not hasattr(creator, "FitnessMin"):
            creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
        if not hasattr(creator, "Individual"):
            # FitnessMin is dynamically created by DEAP
            creator.create("Individual", list, fitness=creator.FitnessMin)  # type: ignore[possibly-missing-attribute]

    def _evaluate_permutation(
        self, permutation: list[int], allocations: tuple[Allocation, ...]
    ) -> tuple[float]:
        """Evaluate a permutation by computing peak memory usage."""

        # Apply permutation
        permuted_allocs = tuple(allocations[i] for i in permutation)

        # Run greedy allocation
        result = super().allocate(permuted_allocs)

        # Calculate peak memory usage
        if not result:
            return (0.0,)

        peak_memory = max(alloc.height for alloc in result if alloc.height is not None)
        return (float(peak_memory),)

    def _create_heuristic_permutations(
        self, allocations: tuple[Allocation, ...]
    ) -> list[list[int]]:
        """Create permutations based on greedy heuristics."""

        permutations = []

        # Create index mapping for original order
        indexed_allocs = list(enumerate(allocations))

        # 1. Greedy by size (largest first)
        sorted_by_size = sorted(indexed_allocs, key=lambda x: x[1].size, reverse=True)
        permutations.append([idx for idx, _ in sorted_by_size])

        return permutations

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        """Evolve permutations using genetic algorithm to find best allocation."""

        # Set random seeds for deterministic behavior
        random.seed(self.seed)
        np.random.default_rng(self.seed)

        if not allocations:
            return allocations

        if len(allocations) == 1:
            return super().allocate(allocations)

        n = len(allocations)

        # Setup toolbox
        toolbox = base.Toolbox()

        # Register individual and population generators
        toolbox.register("indices", random.sample, range(n), n)  # Random permutation
        # Individual and indices are dynamically created by DEAP
        toolbox.register(
            "individual",
            tools.initIterate,
            creator.Individual,  # type: ignore[possibly-missing-attribute]
            toolbox.indices,  # type: ignore[possibly-missing-attribute]
        )

        # Register genetic operators
        toolbox.register(
            "evaluate", self._evaluate_permutation, allocations=allocations
        )
        toolbox.register("mate", tools.cxOrdered)
        toolbox.register("mutate", tools.mutShuffleIndexes, indpb=0.05)
        # TODO(fpedd): Try larger tournsize and selNSGA2
        toolbox.register("select", tools.selTournament, tournsize=self.tournament_size)

        # Create initial population with heuristic seeding
        population = []

        # Add heuristic-based individuals (5 heuristics)
        heuristic_perms = self._create_heuristic_permutations(allocations)
        for perm in heuristic_perms:
            # Individual is dynamically created by DEAP
            individual = creator.Individual(perm)  # type: ignore[possibly-missing-attribute]
            population.append(individual)

        # Fill rest with random individuals
        for _ in range(self.population_size - len(heuristic_perms)):
            # individual() is dynamically registered on toolbox
            individual = toolbox.individual()  # type: ignore[possibly-missing-attribute]
            population.append(individual)

        # Track best individual
        hall_of_fame = tools.HallOfFame(maxsize=1)

        # Setup statistics
        stats = tools.Statistics(key=lambda ind: ind.fitness.values)
        stats.register("min", np.min)
        stats.register("avg", np.mean)

        # Run genetic algorithm
        # TODO(fpedd): Try eaMuPlusLambda and eaMuCommaLambda
        population, _ = algorithms.eaSimple(
            population=population,
            toolbox=toolbox,
            cxpb=self.crossover_prob,
            mutpb=self.mutation_prob,
            ngen=self.num_generations,
            stats=stats,
            halloffame=hall_of_fame,
            verbose=False,
        )

        # Get best permutation
        best_individual = hall_of_fame[0]
        best_permutation = list(best_individual)

        # Apply best permutation and allocate
        permuted_allocs = tuple(allocations[i] for i in best_permutation)
        return super().allocate(permuted_allocs)
