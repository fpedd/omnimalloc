#
# SPDX-License-Identifier: Apache-2.0
#

import math
import random

from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.analysis import ConflictGraph, conflict_graph
from omnimalloc.common.constants import DEFAULT_SEED, DEFAULT_TIMEOUT
from omnimalloc.common.deadline import (
    deadline_expired,
    ensure_valid_timeout,
    make_deadline,
)
from omnimalloc.common.validation import ensure_non_negative, ensure_positive
from omnimalloc.primitives import Allocation

from .greedy import GreedyAllocator


class HillClimbAllocator(GreedyAllocator):
    """Local search over greedy placement orders with simulated annealing.

    Starts from a conflict-weighted greedy order and repeatedly swaps two
    temporal neighbors of a peak allocation, keeping improvements and some not.
    """

    def __init__(
        self,
        max_iterations: int = 100,
        seed: int = DEFAULT_SEED,
        acceptance_temperature: float = 2.0,
        timeout: float | None = DEFAULT_TIMEOUT,
    ) -> None:
        ensure_positive(max_iterations, "max_iterations")
        ensure_non_negative(acceptance_temperature, "acceptance_temperature")
        ensure_valid_timeout(timeout)

        self._max_iterations = max_iterations
        self._seed = seed
        self._acceptance_temperature = acceptance_temperature
        self._timeout = timeout

    def _collect_neighbors(
        self,
        idx: int,
        order: list[int],
        position: list[int],
        graph: ConflictGraph,
        deadline: float | None,
    ) -> tuple[list[int], list[int]]:
        """Collect first and second level temporal neighbors placed before idx.

        Walks the conflict rows rather than scanning every earlier position, so
        the cost is the degrees involved, not the instance size squared.
        """
        first_level = sorted(
            position[other]
            for other in graph.neighbors(order[idx])
            if position[other] < idx
        )
        second_level: set[int] = set()
        for other_pos in first_level:
            if deadline_expired(deadline):
                break
            for candidate in graph.neighbors(order[other_pos]):
                if position[candidate] < other_pos:
                    second_level.add(position[candidate])

        return first_level, sorted(second_level)

    def _propose_swap(
        self,
        order: list[int],
        position: list[int],
        placed: tuple[Allocation, ...],
        current_peak: int,
        rng: random.Random,
        graph: ConflictGraph,
        deadline: float | None,
    ) -> tuple[int, int] | None:
        """Pick two earlier temporal neighbors of a peak allocation to swap."""
        peak_indices = [
            idx
            for idx, alloc in enumerate(placed)
            if alloc.offset is not None and alloc.offset + alloc.size == current_peak
        ]
        if not peak_indices:
            return None

        target_idx = rng.choice(peak_indices)
        first_level, second_level = self._collect_neighbors(
            target_idx, order, position, graph, deadline
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

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        deadline = make_deadline(self._timeout)
        rng = random.Random(self._seed)
        placer = FirstFitPlacer(allocations)
        # Unbudgeted: the search must not degrade into a different heuristic
        # halfway through a large instance. The CSR form keeps that
        # affordable; the id-keyed map costs two orders of magnitude more.
        graph = conflict_graph(allocations, work_budget=None)
        degrees = [graph.degree(i) for i in range(len(allocations))]

        # Start from size * conflicts^2, size, then id for deterministic ordering
        order = sorted(
            range(len(allocations)),
            key=lambda i: (
                allocations[i].size * degrees[i] ** 2,
                allocations[i].size,
                str(allocations[i].id),
            ),
            reverse=True,
        )
        # position[i] is where allocation i sits in `order`; kept in step so
        # neighbor lookups cost a degree, not a scan
        position = [0] * len(order)
        for pos, idx in enumerate(order):
            position[idx] = pos

        def swap_positions(pos1: int, pos2: int) -> None:
            order[pos1], order[pos2] = order[pos2], order[pos1]
            position[order[pos1]], position[order[pos2]] = pos1, pos2

        # Greedy placement preserves order, so placed[i] corresponds to order[i]
        current = tuple(placer.place(order))
        current_peak = placer.peak(order)
        best, best_peak = current, current_peak

        for iteration in range(self._max_iterations):
            if deadline_expired(deadline):
                break
            swap = self._propose_swap(
                order, position, current, current_peak, rng, graph, deadline
            )
            if swap is None:
                continue

            idx1, idx2 = swap
            swap_positions(idx1, idx2)
            # Scoring stays native: only an accepted swap needs the placement
            # itself, and acceptances thin out as the schedule cools
            candidate_peak = placer.peak(order)

            if self._should_accept(candidate_peak, current_peak, iteration, rng):
                current, current_peak = tuple(placer.place(order)), candidate_peak
                if candidate_peak < best_peak:
                    best, best_peak = current, candidate_peak
            else:
                swap_positions(idx1, idx2)

        return best
