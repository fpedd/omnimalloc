#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.analysis import pressure
from omnimalloc.benchmark.sources import BaseSource
from omnimalloc.benchmark.sources.sync_patterns import SYNC_PATTERNS, SyncPatternSource
from omnimalloc.primitives import Allocation


def _signatures(
    allocations: tuple[Allocation, ...],
) -> list[tuple[object, object, int]]:
    return [(a.start, a.end, a.size) for a in allocations]


def test_sync_patterns_is_registered() -> None:
    assert "sync_pattern" in BaseSource.registry()
    assert BaseSource.get("sync_pattern") is SyncPatternSource


@pytest.mark.parametrize("pattern", SYNC_PATTERNS)
def test_sync_patterns_lifetimes_are_valid(pattern: str) -> None:
    source = SyncPatternSource(num_allocations=32, num_threads=4, pattern=pattern)
    allocations = source.get_allocations()
    assert len(allocations) == 32
    for alloc in allocations:
        assert alloc.dim == 4
        assert all(s <= e for s, e in zip(alloc.start, alloc.end, strict=True))
        assert alloc.start != alloc.end


def test_sync_patterns_single_thread_degenerates_to_dim_one() -> None:
    source = SyncPatternSource(num_allocations=8, num_threads=1)
    assert {a.dim for a in source.get_allocations()} == {1}


def test_sync_patterns_independent_threads_share_nothing() -> None:
    source = SyncPatternSource(num_allocations=32, num_threads=4, pattern="independent")
    for alloc in source.get_allocations():
        assert sum(1 for lane in alloc.end if lane > 0) == 1


def test_sync_patterns_dense_propagates_foreign_lanes() -> None:
    source = SyncPatternSource(num_allocations=32, num_threads=2, pattern="dense")
    assert any(min(a.start) > 0 for a in source.get_allocations())


def test_sync_patterns_ring_propagates_around() -> None:
    source = SyncPatternSource(num_allocations=32, num_threads=3, pattern="ring")
    assert any(min(a.end) > 0 for a in source.get_allocations())


def test_sync_patterns_barrier_every_step_is_lockstep() -> None:
    source = SyncPatternSource(
        num_allocations=16, num_threads=3, pattern="barrier", sync_period=1
    )
    for alloc in source.get_allocations():
        assert len(set(alloc.start)) == 1
        assert len(set(alloc.end)) == 1


def test_sync_patterns_groups_stay_isolated_before_global_barrier() -> None:
    source = SyncPatternSource(
        num_allocations=32,
        num_threads=4,
        pattern="groups",
        sync_period=2,
        group_size=2,
        steps=7,
    )
    for alloc in source.get_allocations():
        assert alloc.end[2:] == (0, 0) or alloc.end[:2] == (0, 0)


def test_sync_patterns_is_deterministic_per_seed() -> None:
    a = SyncPatternSource(num_allocations=32, seed=7).get_allocations()
    b = SyncPatternSource(num_allocations=32, seed=7).get_allocations()
    c = SyncPatternSource(num_allocations=32, seed=8).get_allocations()
    assert _signatures(a) == _signatures(b)
    assert _signatures(a) != _signatures(c)


def test_sync_patterns_distinct_pools_differ() -> None:
    source = SyncPatternSource(num_allocations=16)
    pools = source.get_pools(num_pools=2)
    assert _signatures(pools[0].allocations) != _signatures(pools[1].allocations)


def test_sync_patterns_rejects_unknown_pattern() -> None:
    with pytest.raises(ValueError, match="pattern"):
        SyncPatternSource(pattern="mesh")


def test_sync_patterns_rejects_nonpositive_threads() -> None:
    with pytest.raises(ValueError, match="num_threads"):
        SyncPatternSource(num_threads=0)


def test_sync_patterns_rejects_nonpositive_sync_period() -> None:
    with pytest.raises(ValueError, match="sync_period"):
        SyncPatternSource(sync_period=0)


def test_sync_patterns_rejects_too_few_steps() -> None:
    with pytest.raises(ValueError, match="steps"):
        SyncPatternSource(steps=1)


@pytest.mark.parametrize("pattern", SYNC_PATTERNS)
def test_sync_patterns_pressure_is_bounded(pattern: str) -> None:
    source = SyncPatternSource(
        num_allocations=10, num_threads=3, pattern=pattern, seed=5
    )
    allocations = source.get_allocations()
    peak = pressure(allocations)
    assert peak >= max(a.size for a in allocations)
    assert peak <= sum(a.size for a in allocations)
