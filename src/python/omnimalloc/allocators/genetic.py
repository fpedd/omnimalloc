#
# SPDX-License-Identifier: Apache-2.0
#

import random
import threading
from typing import Any, cast

from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.common.constants import DEFAULT_SEED, DEFAULT_TIMEOUT
from omnimalloc.common.deadline import (
    deadline_expired,
    ensure_valid_timeout,
    make_deadline,
)
from omnimalloc.common.optional import require_optional
from omnimalloc.common.validation import ensure_non_negative, ensure_positive
from omnimalloc.primitives import Allocation

from .greedy import (
    GreedyAllocator,
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


# DEAP's operators are hardwired to the global random module, so the seeded
# section is a process-wide critical section rather than per-instance state
_GLOBAL_RNG_LOCK = threading.Lock()


class GeneticAllocator(GreedyAllocator):
    """Genetic algorithm allocator that evolves greedy placement orders.

    `timeout` (default 3s) bounds wall-clock time between generations,
    independent of `max_generations`; set it to None to disable the deadline.
    """

    def __init__(
        self,
        seed: int = DEFAULT_SEED,
        population_size: int = 100,
        max_generations: int = 50,
        crossover_prob: float = 0.7,
        mutation_prob: float = 0.2,
        tournament_size: int = 3,
        timeout: float | None = DEFAULT_TIMEOUT,
    ) -> None:
        if not HAS_DEAP:
            require_optional("deap", "GeneticAllocator")
        ensure_positive(population_size, "population_size")
        ensure_non_negative(max_generations, "max_generations")
        if not 0.0 <= crossover_prob <= 1.0 or not 0.0 <= mutation_prob <= 1.0:
            raise ValueError(
                f"crossover_prob and mutation_prob must be in [0, 1], "
                f"got {crossover_prob} and {mutation_prob}"
            )
        ensure_positive(tournament_size, "tournament_size")
        ensure_valid_timeout(timeout)

        self._seed = seed
        self._population_size = population_size
        self._max_generations = max_generations
        self._crossover_prob = crossover_prob
        self._mutation_prob = mutation_prob
        self._tournament_size = tournament_size
        self._timeout = timeout

        # Setup DEAP creators (only once per process, they live in a global
        # namespace); namespaced names so user-created DEAP classes with other
        # objectives cannot be silently reused
        if not hasattr(creator, "OmnimallocFitnessMin"):
            creator.create("OmnimallocFitnessMin", base.Fitness, weights=(-1.0,))
        if not hasattr(creator, "OmnimallocIndividual"):
            # OmnimallocFitnessMin is dynamically created by DEAP
            creator.create(
                "OmnimallocIndividual",
                list,
                fitness=creator.OmnimallocFitnessMin,  # ty: ignore[unresolved-attribute]
            )

    def _evaluate_permutation(
        self, permutation: list[int], placer: FirstFitPlacer
    ) -> tuple[float]:
        """Evaluate a permutation by computing its greedy peak memory usage."""
        return (float(placer.peak(permutation)),)

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
        return permutations[: self._population_size]

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        """Evolve permutations using a genetic algorithm to find best allocation."""
        if len(allocations) < 2:
            return super()._allocate(allocations)

        # DEAP operators draw from the global random module, so the seeding is
        # process-wide: seed, restore the caller's stream afterwards, and lock
        # so concurrent calls cannot interleave draws or saved state.
        with _GLOBAL_RNG_LOCK:
            random_state = random.getstate()
            random.seed(self._seed)
            try:
                return self._evolve(allocations)
            finally:
                random.setstate(random_state)

    def _evolve(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        # Started before the placer and the seed orders: they are the first
        # thing a large instance spends its budget on
        deadline = make_deadline(self._timeout)
        placer = FirstFitPlacer(allocations)
        toolbox = base.Toolbox()
        n = len(allocations)
        toolbox.register("indices", random.sample, range(n), n)
        # OmnimallocIndividual and indices are dynamically created by DEAP
        toolbox.register(
            "individual",
            tools.initIterate,
            creator.OmnimallocIndividual,  # ty: ignore[unresolved-attribute]
            toolbox.indices,  # ty: ignore[unresolved-attribute]
        )
        toolbox.register("evaluate", self._evaluate_permutation, placer=placer)
        toolbox.register("mate", tools.cxOrdered)
        toolbox.register("mutate", tools.mutShuffleIndexes, indpb=0.05)
        # TODO(fpedd): Try larger tournsize and selNSGA2
        toolbox.register("select", tools.selTournament, tournsize=self._tournament_size)

        # Seed the population with heuristic orders, fill up with random ones
        # OmnimallocIndividual and individual() are dynamically created by DEAP
        population = [
            creator.OmnimallocIndividual(permutation)  # ty: ignore[unresolved-attribute]
            for permutation in self._heuristic_permutations(allocations)
        ]
        population += [
            toolbox.individual()  # ty: ignore[unresolved-attribute]
            for _ in range(self._population_size - len(population))
        ]

        hall_of_fame = tools.HallOfFame(maxsize=1)

        def evaluate_invalid(individuals: list[Any]) -> list[Any]:
            """Score until the budget runs out; returns the ones that got one.

            One evaluation is a full greedy placement, so a population dwarfs
            the budget on a large instance. The first individual always runs.
            """
            scored = []
            for individual in individuals:
                if not individual.fitness.valid:
                    if scored and deadline_expired(deadline):
                        break
                    individual.fitness.values = toolbox.evaluate(individual)  # ty: ignore[unresolved-attribute]
                scored.append(individual)
            return scored

        # DEAP's eaSimple, unrolled so a wall-clock deadline can stop between
        # generations; varAnd keeps the RNG stream identical to eaSimple.
        # TODO(fpedd): Try eaMuPlusLambda and eaMuCommaLambda
        hall_of_fame.update(evaluate_invalid(population))
        for _ in range(self._max_generations):
            if deadline_expired(deadline):
                break
            offspring = toolbox.select(population, len(population))  # ty: ignore[unresolved-attribute]
            offspring = algorithms.varAnd(
                offspring, toolbox, self._crossover_prob, self._mutation_prob
            )
            scored = evaluate_invalid(offspring)
            hall_of_fame.update(scored)
            if len(scored) < len(offspring):
                break  # budget gone mid-generation; the rest are unscored
            population[:] = offspring

        best_permutation = list(hall_of_fame[0])
        return tuple(placer.place(best_permutation))
