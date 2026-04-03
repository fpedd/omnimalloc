#
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .analyzers import Partition, Section, get_overlaps

if TYPE_CHECKING:
    from omnimalloc.primitives.allocation import Allocation


@dataclass
class SolverProblem:
    n: int
    m: int
    sizes: list[int]
    section_allocs: list[list[int]]
    section_totals: list[int]
    alloc_sections: list[list[int]]
    overlaps: list[list[int]]
    allocations: list[Allocation]
    lower_bound: int


@dataclass
class SearchState:
    offsets: list[int | None]
    section_floors: list[int]
    section_unalloc_totals: list[int]
    capacity: int
    best_offsets: list[int | None]
    steps: int
    budget: int
    floor_stacks: list[list[int]] = field(default_factory=list)


def build_solver_problem(
    partition: Partition, order: list[Allocation]
) -> SolverProblem:
    n = len(order)
    alloc_to_idx: dict[Allocation, int] = {a: i for i, a in enumerate(order)}

    # Sort sections temporally
    sorted_sections: list[Section] = sorted(
        partition.sections,
        key=lambda s: min(a.start for a in s.allocations),
    )
    m = len(sorted_sections)

    sizes = [a.size for a in order]

    # Build section -> alloc indices and alloc -> section indices
    section_allocs: list[list[int]] = []
    alloc_sections: list[list[int]] = [[] for _ in range(n)]
    section_totals: list[int] = []

    for j, section in enumerate(sorted_sections):
        indices = sorted(
            alloc_to_idx[a] for a in section.allocations if a in alloc_to_idx
        )
        section_allocs.append(indices)
        section_totals.append(sum(sizes[i] for i in indices))
        for i in indices:
            alloc_sections[i].append(j)

    # Build overlaps from analyzer
    alloc_tuple = tuple(order)
    overlap_map = get_overlaps(alloc_tuple)
    overlaps: list[list[int]] = [[] for _ in range(n)]
    for alloc, others in overlap_map.items():
        if alloc in alloc_to_idx:
            i = alloc_to_idx[alloc]
            overlaps[i] = sorted(alloc_to_idx[o] for o in others if o in alloc_to_idx)

    lower_bound = max(section_totals) if section_totals else 0

    return SolverProblem(
        n=n,
        m=m,
        sizes=sizes,
        section_allocs=section_allocs,
        section_totals=section_totals,
        alloc_sections=alloc_sections,
        overlaps=overlaps,
        allocations=list(order),
        lower_bound=lower_bound,
    )


def static_preorder(problem: SolverProblem) -> list[int]:
    def key(i: int) -> tuple[int, int, int]:
        max_section_total = max(
            (problem.section_totals[j] for j in problem.alloc_sections[i]),
            default=0,
        )
        return (max_section_total, problem.sizes[i], len(problem.overlaps[i]))

    indices = list(range(problem.n))
    indices.sort(key=key, reverse=True)
    return indices


def solve(problem: SolverProblem, budget: int) -> list[int]:
    if problem.n == 0:
        return []

    order = static_preorder(problem)

    state = SearchState(
        offsets=[None] * problem.n,
        section_floors=[0] * problem.m,
        section_unalloc_totals=list(problem.section_totals),
        capacity=sys.maxsize,
        best_offsets=[None] * problem.n,
        steps=0,
        budget=budget,
        floor_stacks=[[] for _ in range(problem.m)],
    )

    _dfs(problem, state, order, 0)

    # If no solution found (shouldn't happen with infinite capacity), fall back
    if all(o is None for o in state.best_offsets):
        return [0] * problem.n

    return [o if o is not None else 0 for o in state.best_offsets]


def _record_if_better(problem: SolverProblem, state: SearchState) -> None:
    height = max(
        state.offsets[i] + problem.sizes[i]  # type: ignore[operator]
        for i in range(problem.n)
    )
    if height < state.capacity:
        state.capacity = height
        state.best_offsets = list(state.offsets)


def _get_placed_overlaps(
    problem: SolverProblem, state: SearchState, alloc_idx: int
) -> list[tuple[int, int]]:
    placed: list[tuple[int, int]] = []
    for j in problem.overlaps[alloc_idx]:
        off_j = state.offsets[j]
        if off_j is not None:
            placed.append((off_j, problem.sizes[j]))
    placed.sort()
    return placed


def _should_stop(problem: SolverProblem, state: SearchState) -> bool:
    return state.steps >= state.budget or state.capacity <= problem.lower_bound


def _dfs(
    problem: SolverProblem,
    state: SearchState,
    order: list[int],
    depth: int,
) -> None:
    if state.steps >= state.budget:
        return
    state.steps += 1

    if depth == len(order):
        _record_if_better(problem, state)
        return

    if state.capacity <= problem.lower_bound:
        return

    alloc_idx = order[depth]
    size_i = problem.sizes[alloc_idx]
    placed_overlaps = _get_placed_overlaps(problem, state, alloc_idx)
    candidates = _compute_candidates(size_i, placed_overlaps)

    for offset in candidates:
        if offset + size_i >= state.capacity:
            break

        if not _section_feasible(problem, state, alloc_idx, offset, size_i):
            continue

        _place(problem, state, alloc_idx, offset)
        _dfs(problem, state, order, depth + 1)
        _unplace(problem, state, alloc_idx)

        if _should_stop(problem, state):
            return

        if _is_hatless(problem, state, alloc_idx):
            break


def _compute_candidates(size: int, placed_overlaps: list[tuple[int, int]]) -> list[int]:
    candidate_set: set[int] = {0}
    for off, sz in placed_overlaps:
        candidate_set.add(off + sz)

    # Filter: only keep offsets that don't conflict with any placed overlap
    valid: list[int] = []
    for c in sorted(candidate_set):
        end = c + size
        conflict = False
        for off, sz in placed_overlaps:
            if c < off + sz and off < end:
                conflict = True
                break
        if not conflict:
            valid.append(c)

    return valid


def _section_feasible(
    problem: SolverProblem,
    state: SearchState,
    alloc_idx: int,
    offset: int,
    size: int,
) -> bool:
    height = offset + size
    for j in problem.alloc_sections[alloc_idx]:
        new_floor = max(state.section_floors[j], height)
        remaining = state.section_unalloc_totals[j] - size
        if new_floor + remaining >= state.capacity:
            return False
    return True


def _place(
    problem: SolverProblem,
    state: SearchState,
    alloc_idx: int,
    offset: int,
) -> None:
    state.offsets[alloc_idx] = offset
    height = offset + problem.sizes[alloc_idx]
    for j in problem.alloc_sections[alloc_idx]:
        state.floor_stacks[j].append(state.section_floors[j])
        state.section_floors[j] = max(state.section_floors[j], height)
        state.section_unalloc_totals[j] -= problem.sizes[alloc_idx]


def _unplace(
    problem: SolverProblem,
    state: SearchState,
    alloc_idx: int,
) -> None:
    state.offsets[alloc_idx] = None
    # Restore floors and totals (reverse order for stack correctness)
    for j in reversed(problem.alloc_sections[alloc_idx]):
        state.section_floors[j] = state.floor_stacks[j].pop()
        state.section_unalloc_totals[j] += problem.sizes[alloc_idx]


def _is_hatless(
    problem: SolverProblem,
    state: SearchState,
    alloc_idx: int,
) -> bool:
    for j in problem.overlaps[alloc_idx]:
        if state.offsets[j] is None:
            return False
    return True
