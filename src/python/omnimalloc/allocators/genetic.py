#
# SPDX-License-Identifier: Apache-2.0
#

import random
import time
from typing import Any, cast

from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.common.optional import require_optional
from omnimalloc.primitives import Allocation

from .base import DEFAULT_TIMEOUT
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
    """Genetic algorithm allocator that evolves greedy placement orders.

    `timeout` (default 3s) bounds wall-clock time between generations,
    independent of `num_generations`; set it to 0 to disable the deadline.
    """

    def __init__(
        self,
        seed: int = 42,
        population_size: int = 100,
        num_generations: int = 50,
        crossover_prob: float = 0.7,
        mutation_prob: float = 0.2,
        tournament_size: int = 3,
        timeout: float = DEFAULT_TIMEOUT,
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
        if timeout < 0:
            raise ValueError(f"timeout must be non-negative, got {timeout}")

        self.seed = seed
        self.population_size = population_size
        self.num_generations = num_generations
        self.crossover_prob = crossover_prob
        self.mutation_prob = mutation_prob
        self.tournament_size = tournament_size
        self.timeout = timeout

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

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        """Evolve permutations using a genetic algorithm to find best allocation."""
        if len(allocations) < 2:
            return super()._allocate(allocations)

        # DEAP operators draw from the global random module; seed it for
        # determinism but restore the caller's stream afterwards.
        random_state = random.getstate()
        random.seed(self.seed)
        try:
            return self._evolve(allocations)
        finally:
            random.setstate(random_state)

    def _evolve(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
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

        def evaluate_invalid(individuals: list[Any]) -> None:
            for individual in individuals:
                if not individual.fitness.valid:
                    individual.fitness.values = toolbox.evaluate(individual)  # type: ignore[unresolved-attribute]

        # DEAP's eaSimple, unrolled so a wall-clock deadline can stop between
        # generations; varAnd keeps the RNG stream identical to eaSimple.
        # TODO(fpedd): Try eaMuPlusLambda and eaMuCommaLambda
        deadline = time.monotonic() + self.timeout if self.timeout else None
        evaluate_invalid(population)
        hall_of_fame.update(population)
        for _ in range(self.num_generations):
            if deadline is not None and time.monotonic() >= deadline:
                break
            offspring = toolbox.select(population, len(population))  # type: ignore[unresolved-attribute]
            offspring = algorithms.varAnd(
                offspring, toolbox, self.crossover_prob, self.mutation_prob
            )
            evaluate_invalid(offspring)
            hall_of_fame.update(offspring)
            population[:] = offspring

        best_permutation = list(hall_of_fame[0])
        return tuple(placer.place(best_permutation))
