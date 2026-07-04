#
# SPDX-License-Identifier: Apache-2.0
#

import math
import random

from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.primitives import Allocation

from .greedy import GreedyAllocator
from .greedy_base import compute_conflicts, peak_memory


class HillClimbAllocator(GreedyAllocator):
    """Local search over greedy placement orders with simulated annealing.

    Starts from a conflict-weighted greedy order and repeatedly swaps two
    temporal neighbors of a peak allocation, keeping the swap if the greedy
    peak improves (or occasionally when it worsens, per the annealing
    schedule). `acceptance_temperature` is the percent worsening accepted
    with probability 1/e at the start; it cools linearly to zero.
    """

    def __init__(
        self,
        max_iterations: int = 100,
        seed: int = 42,
        acceptance_temperature: float = 2.0,
    ) -> None:
        if max_iterations <= 0:
            raise ValueError(f"max_iterations must be positive, got {max_iterations}")
        if acceptance_temperature < 0:
            raise ValueError(
                f"acceptance_temperature must be non-negative, "
                f"got {acceptance_temperature}"
            )

        self._max_iterations = max_iterations
        self._seed = seed
        self._acceptance_temperature = acceptance_temperature

    def _collect_neighbors(
        self,
        idx: int,
        order: list[int],
        allocations: tuple[Allocation, ...],
        adjacency: dict[int | str, set[int | str]],
    ) -> tuple[list[int], list[int]]:
        """Collect first and second level temporal neighbors placed before idx."""
        first_level: set[int] = set()
        second_level: set[int] = set()
        adjacent = adjacency.get(allocations[order[idx]].id, frozenset())

        for other_pos in range(idx):
            other = allocations[order[other_pos]]
            if other.id in adjacent:
                first_level.add(other_pos)

                other_adjacent = adjacency.get(other.id, frozenset())
                for candidate_pos in range(other_pos):
                    if allocations[order[candidate_pos]].id in other_adjacent:
                        second_level.add(candidate_pos)

        return sorted(first_level), sorted(second_level)

    def _propose_swap(
        self,
        order: list[int],
        placed: tuple[Allocation, ...],
        peak: int,
        rng: random.Random,
        allocations: tuple[Allocation, ...],
        adjacency: dict[int | str, set[int | str]],
    ) -> tuple[int, int] | None:
        """Pick two earlier temporal neighbors of a peak allocation to swap."""
        peak_indices = [
            idx
            for idx, alloc in enumerate(placed)
            if alloc.offset is not None and alloc.offset + alloc.size == peak
        ]
        if not peak_indices:
            return None

        target_idx = rng.choice(peak_indices)
        first_level, second_level = self._collect_neighbors(
            target_idx, order, allocations, adjacency
        )
        if not first_level:
            return None

        idx1 = rng.choice(first_level)
        # Favor reaching further back to escape local rearrangements
        use_second = bool(second_level) and rng.random() < 0.75
        idx2 = rng.choice(second_level if use_second else first_level)
        return (idx1, idx2) if idx1 != idx2 else None

    def _should_accept(
        self, candidate: int, current: int, iteration: int, rng: random.Random
    ) -> bool:
        """Accept improvements always, worsenings per the annealing schedule."""
        if candidate <= current:
            return True

        cooling = 1.0 - iteration / self._max_iterations
        temperature = self._acceptance_temperature * cooling
        if temperature <= 0.0:
            return False

        worsening_percent = 100.0 * (candidate - current) / current
        return rng.random() < math.exp(-worsening_percent / temperature)

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations

        rng = random.Random(self._seed)
        conflicts = compute_conflicts(allocations)
        placer = FirstFitPlacer(list(allocations))
        adjacency = placer.overlaps

        # Start from size * conflicts^2, size, then id for deterministic ordering
        order = sorted(
            range(len(allocations)),
            key=lambda i: (
                allocations[i].size * conflicts[allocations[i]] ** 2,
                allocations[i].size,
                str(allocations[i].id),
            ),
            reverse=True,
        )

        # Greedy placement preserves order, so placed[i] corresponds to order[i]
        current = tuple(placer.place(order))
        current_peak = peak_memory(current)
        best, best_peak = current, current_peak

        for iteration in range(self._max_iterations):
            swap = self._propose_swap(
                order, current, current_peak, rng, allocations, adjacency
            )
            if swap is None:
                continue

            idx1, idx2 = swap
            order[idx1], order[idx2] = order[idx2], order[idx1]
            candidate = tuple(placer.place(order))
            candidate_peak = peak_memory(candidate)

            if self._should_accept(candidate_peak, current_peak, iteration, rng):
                current, current_peak = candidate, candidate_peak
                if candidate_peak < best_peak:
                    best, best_peak = candidate, candidate_peak
            else:
                order[idx1], order[idx2] = order[idx2], order[idx1]

        return best
