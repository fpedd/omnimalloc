#
# SPDX-License-Identifier: Apache-2.0
#

import random

from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class HillClimbAllocator(BaseAllocator):
    """Allocator using hill climbing with simulated annealing."""

    def __init__(
        self,
        max_iterations: int = 100,
        seed: int = 42,
        acceptance_temperature: float = 50.0,
    ) -> None:
        if max_iterations <= 0:
            raise ValueError(f"max_iterations must be positive, got {max_iterations}")
        if acceptance_temperature < 0:
            raise ValueError(
                f"acceptance_temperature must be non-negative, "
                f"got {acceptance_temperature}"
            )

        self.max_iterations = max_iterations
        self.seed = seed
        self.acceptance_temperature = acceptance_temperature

    def _compute_allocation_score(self, alloc: Allocation, conflicts: int) -> float:
        return float(alloc.size) * float(conflicts * conflicts)

    def _greedy_allocate(self, allocations: list[Allocation]) -> tuple[Allocation, ...]:
        """Place allocations greedily by finding first available offset."""
        placed: list[Allocation] = []

        for alloc in allocations:
            overlapping = [p for p in placed if alloc.overlaps_temporally(p)]
            overlapping.sort(key=lambda a: a.offset or 0)

            offset = 0
            for placed_alloc in overlapping:
                assert placed_alloc.offset is not None
                if placed_alloc.offset - offset >= alloc.size:
                    break
                offset = max(offset, placed_alloc.offset + placed_alloc.size)

            placed.append(alloc.with_offset(offset))

        return tuple(placed)

    def _calculate_total_memory(self, allocations: tuple[Allocation, ...]) -> int:
        if not allocations:
            return 0
        offsets_with_sizes = [
            a.offset + a.size for a in allocations if a.offset is not None
        ]
        return max(offsets_with_sizes) if offsets_with_sizes else 0

    def _count_conflicts(
        self, allocations: tuple[Allocation, ...]
    ) -> dict[str | int, int]:
        """Count temporal overlaps for each allocation."""
        counts = {}
        for alloc in allocations:
            conflicts = sum(
                1
                for other in allocations
                if other.id != alloc.id and alloc.overlaps_temporally(other)
            )
            counts[alloc.id] = conflicts
        return counts

    def _collect_neighbors(
        self, idx: int, allocations: list[Allocation]
    ) -> tuple[list[int], list[int]]:
        """Collect first and second level temporal neighbors."""
        first_level = set()
        second_level = set()
        alloc = allocations[idx]

        for other_idx in range(idx):
            other = allocations[other_idx]
            if alloc.overlaps_temporally(other):
                first_level.add(other_idx)

                for candidate_idx in range(other_idx):
                    if other.overlaps_temporally(allocations[candidate_idx]):
                        second_level.add(candidate_idx)

        return sorted(first_level), sorted(second_level)

    def _should_accept(
        self, current: int, best: int, iteration: int, rng: random.Random
    ) -> bool:
        """Determine if allocation should be accepted using simulated annealing."""
        if iteration == 0 or current <= best:
            return True

        delta = current - best
        probability = int(
            self.acceptance_temperature * delta / current / (iteration + 1)
        )
        return rng.randint(0, 99) < probability

    def _find_max_memory_allocations(
        self, allocations: tuple[Allocation, ...], total_memory: int
    ) -> list[Allocation]:
        """Find allocations that end at the maximum memory position."""
        return [
            a
            for a in allocations
            if a.offset is not None and a.offset + a.size == total_memory
        ]

    def _select_swap_candidates(
        self, first_level: list[int], second_level: list[int], rng: random.Random
    ) -> tuple[int, int]:
        """Select two indices to swap from neighbor lists."""
        idx1 = rng.choice(first_level)
        idx2 = idx1

        # Try to find different index
        for _ in range(10):
            if second_level and (not first_level or rng.randint(0, 99) > 25):
                idx2 = rng.choice(second_level)
            else:
                idx2 = rng.choice(first_level)

            if idx2 != idx1:
                break

            if len(second_level) < 2 and len(first_level) < 2:
                break

        return idx1, idx2

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations

        rng = random.Random(self.seed)
        conflict_counts = self._count_conflicts(allocations)

        # Sort by score, size, then id for deterministic ordering
        alloc_list = sorted(
            allocations,
            key=lambda a: (
                self._compute_allocation_score(a, conflict_counts[a.id]),
                a.size,
                str(a.id),
            ),
            reverse=True,
        )

        pos_map = {alloc.id: idx for idx, alloc in enumerate(alloc_list)}

        # Initial greedy allocation
        result = self._greedy_allocate(alloc_list)
        best_memory = self._calculate_total_memory(result)
        best_result = result

        for iteration in range(self.max_iterations):
            rollback_pos_map = pos_map.copy()

            result = self._greedy_allocate(alloc_list)
            current_memory = self._calculate_total_memory(result)

            # Accept or reject based on simulated annealing
            if self._should_accept(current_memory, best_memory, iteration, rng):
                best_result = result
                best_memory = current_memory
            elif iteration > 0:
                # Undo last swap
                swap_idx1 = next(
                    i for i, a in enumerate(alloc_list) if pos_map[a.id] != i
                )
                swap_idx2 = pos_map[alloc_list[swap_idx1].id]
                alloc_list[swap_idx1], alloc_list[swap_idx2] = (
                    alloc_list[swap_idx2],
                    alloc_list[swap_idx1],
                )
                pos_map = rollback_pos_map

            # Find allocations at maximum memory and select target
            max_allocs = self._find_max_memory_allocations(result, current_memory)
            if not max_allocs:
                continue

            target = rng.choice(max_allocs)
            target_idx = pos_map[target.id]

            # Collect neighbors and select swap candidates
            first_level, second_level = self._collect_neighbors(target_idx, alloc_list)
            if not first_level:
                continue

            swap_idx1, swap_idx2 = self._select_swap_candidates(
                first_level, second_level, rng
            )
            if swap_idx1 == swap_idx2:
                continue

            # Perform swap
            alloc_list[swap_idx1], alloc_list[swap_idx2] = (
                alloc_list[swap_idx2],
                alloc_list[swap_idx1],
            )
            pos_map[alloc_list[swap_idx1].id] = swap_idx1
            pos_map[alloc_list[swap_idx2].id] = swap_idx2

        return best_result
