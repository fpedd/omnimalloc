#
# SPDX-License-Identifier: Apache-2.0
#

import itertools
import random

import pytest
from omnimalloc import _cpp, validate_allocation
from omnimalloc.allocators import OmniAllocator
from omnimalloc.benchmark.sources import SyncPatternSource
from omnimalloc.benchmark.sources.sync_patterns import SYNC_PATTERNS
from omnimalloc.primitives import Allocation, Pool


def componentwise_le(a: tuple[int, ...], b: tuple[int, ...]) -> bool:
    return all(x <= y for x, y in zip(a, b, strict=True))


def oracle_conflict(
    start_a: tuple[int, ...],
    end_a: tuple[int, ...],
    start_b: tuple[int, ...],
    end_b: tuple[int, ...],
) -> bool:
    return not componentwise_le(end_a, start_b) and not componentwise_le(end_b, start_a)


def make_allocation(
    idx: int,
    start: tuple[int, ...],
    end: tuple[int, ...],
    offset: int | None = None,
) -> Allocation:
    return Allocation(
        id=idx,
        size=8,
        start=start if len(start) > 1 else start[0],
        end=end if len(end) > 1 else end[0],
        offset=offset,
    )


def random_lifetime(
    rng: random.Random, dim: int, hi: int = 5
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    while True:
        start = tuple(rng.randint(0, hi) for _ in range(dim))
        end = tuple(rng.randint(start[t], hi + 2) for t in range(dim))
        if end != start:
            return start, end


def random_execution(
    rng: random.Random, threads: int, per_thread: int, syncs: int
) -> tuple[list[tuple[int, ...]], list[set[int]]]:
    num_events = threads * per_thread
    event_thread = [t for t in range(threads) for _ in range(per_thread)]
    event_pos = [k for _ in range(threads) for k in range(per_thread)]
    preds: list[list[int]] = [[] for _ in range(num_events)]
    for event in range(num_events):
        if event_pos[event] > 0:
            preds[event].append(event - 1)
    for _ in range(syncs):
        src, dst = rng.randrange(num_events), rng.randrange(num_events)
        if event_thread[src] != event_thread[dst] and src < dst:
            preds[dst].append(src)
    clocks: list[tuple[int, ...]] = []
    ancestors: list[set[int]] = []
    for event in range(num_events):
        clock = [0] * threads
        reach = {event}
        for pred in preds[event]:
            clock = [max(x, y) for x, y in zip(clock, clocks[pred], strict=True)]
            reach |= ancestors[pred]
        clock[event_thread[event]] += 1
        clocks.append(tuple(clock))
        ancestors.append(reach)
    return clocks, ancestors


@pytest.mark.parametrize("dim", [1, 2, 3, 5])
def test_pairwise_overlap_matches_oracle(dim: int) -> None:
    rng = random.Random(dim)
    for _ in range(1_000):
        start_a, end_a = random_lifetime(rng, dim)
        start_b, end_b = random_lifetime(rng, dim)
        a = make_allocation(0, start_a, end_a)
        b = make_allocation(1, start_b, end_b)
        expected = oracle_conflict(start_a, end_a, start_b, end_b)
        assert a.conflicts_with(b) == expected
        assert b.conflicts_with(a) == expected


def test_equal_end_start_boundary_is_safe() -> None:
    a = make_allocation(0, (2, 3), (5, 7))
    b = make_allocation(1, (5, 7), (6, 8))
    assert not a.conflicts_with(b)


def test_single_earlier_component_conflicts() -> None:
    a = make_allocation(0, (2, 3), (5, 7))
    b = make_allocation(1, (5, 6), (6, 8))
    assert a.conflicts_with(b)


def test_incomparable_clocks_conflict() -> None:
    a = make_allocation(0, (2, 3), (5, 7))
    b = make_allocation(1, (4, 8), (9, 9))
    assert a.conflicts_with(b)


def test_dominated_lifetime_is_safe() -> None:
    a = make_allocation(0, (2, 3), (5, 7))
    b = make_allocation(1, (6, 7), (7, 8))
    assert not a.conflicts_with(b)


@pytest.mark.parametrize("hi", [3, 6, 20])
def test_conflict_graph_matches_oracle(hi: int) -> None:
    rng = random.Random(hi)
    for _ in range(15):
        dim = rng.choice([2, 3, 4, 8])
        count = rng.randint(2, 80)
        lifetimes = [random_lifetime(rng, dim, hi=hi) for _ in range(count)]
        allocations = [
            make_allocation(i, start, end) for i, (start, end) in enumerate(lifetimes)
        ]
        graph = _cpp.conflicts(allocations, None)
        for i, j in itertools.combinations(range(count), 2):
            expected = oracle_conflict(*lifetimes[i], *lifetimes[j])
            assert (j in graph.get(i, set())) == expected
            assert (i in graph.get(j, set())) == expected


def test_same_offset_reuse_across_happens_before_passes() -> None:
    first = make_allocation(0, (0, 0), (2, 1), offset=0)
    second = make_allocation(1, (2, 1), (3, 3), offset=0)
    validate_allocation(Pool(id=0, allocations=(first, second)))


def test_one_flipped_component_fails_validation() -> None:
    first = make_allocation(0, (0, 0), (2, 1), offset=0)
    second = make_allocation(1, (2, 0), (3, 3), offset=0)
    with pytest.raises(ValueError, match="overlaps"):
        validate_allocation(Pool(id=0, allocations=(first, second)))


@pytest.mark.parametrize("pattern", sorted(SYNC_PATTERNS))
def test_validator_catches_corrupted_placements(pattern: str) -> None:
    rng = random.Random(3)
    allocations = SyncPatternSource(
        num_allocations=200, num_threads=6, pattern=pattern, seed=13
    ).get_allocations()
    placed = list(OmniAllocator().allocate(allocations))
    validate_allocation(Pool(id=0, allocations=tuple(placed)))
    conflict_map = _cpp.conflicts(placed, None)
    index_by_id = {p.id: k for k, p in enumerate(placed)}
    conflicting = sorted(a for a, neighbors in conflict_map.items() if neighbors)
    for _ in range(5):
        i = rng.choice(conflicting)
        j = rng.choice(sorted(conflict_map[i]))
        corrupted = list(placed)
        corrupted[index_by_id[j]] = placed[index_by_id[j]].with_offset(
            placed[index_by_id[i]].offset
        )
        with pytest.raises(ValueError, match="overlaps"):
            validate_allocation(Pool(id=0, allocations=tuple(corrupted)))


def test_vector_clock_order_equals_causal_reachability() -> None:
    rng = random.Random(2)
    for _ in range(10):
        threads = rng.randint(2, 5)
        clocks, ancestors = random_execution(
            rng, threads, rng.randint(3, 6), syncs=rng.randint(0, 20)
        )
        for e1, e2 in itertools.permutations(range(len(clocks)), 2):
            assert componentwise_le(clocks[e1], clocks[e2]) == (e1 in ancestors[e2])


def test_overlap_equals_possible_co_liveness_in_executions() -> None:
    rng = random.Random(4)
    for _ in range(10):
        threads = rng.randint(2, 5)
        clocks, ancestors = random_execution(
            rng, threads, rng.randint(3, 6), syncs=rng.randint(0, 20)
        )
        num_events = len(clocks)
        lifetimes = []
        for _ in range(10):
            alloc_ev = rng.randrange(num_events)
            frees = [
                e
                for e in range(num_events)
                if alloc_ev in ancestors[e] and e != alloc_ev
            ]
            if frees:
                lifetimes.append((alloc_ev, rng.choice(frees)))
        for (alloc_a, free_a), (alloc_b, free_b) in itertools.combinations(
            lifetimes, 2
        ):
            a = make_allocation(0, clocks[alloc_a], clocks[free_a])
            b = make_allocation(1, clocks[alloc_b], clocks[free_b])
            causally_ordered = (
                free_a in ancestors[alloc_b] or free_b in ancestors[alloc_a]
            )
            assert a.conflicts_with(b) == (not causally_ordered)
            if not causally_ordered:
                down_closure = ancestors[alloc_a] | ancestors[alloc_b]
                assert free_a not in down_closure
                assert free_b not in down_closure
