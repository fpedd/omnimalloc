#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.analysis import antichain_pressure, try_linearize
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
    with pytest.raises(ValueError, match="not a valid SyncPattern"):
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
    peak = antichain_pressure(allocations)
    assert peak >= max(a.size for a in allocations)
    assert peak <= sum(a.size for a in allocations)


def test_every_pattern_runs_at_a_high_thread_count() -> None:
    for pattern in SYNC_PATTERNS:
        source = SyncPatternSource(
            num_allocations=64, num_threads=64, pattern=pattern, seed=3
        )
        allocations = source.get_allocations()
        assert len(allocations) == 64
        assert all(a.dim == 64 for a in allocations), pattern


def test_speed_skew_staggers_the_thread_clocks() -> None:
    source = SyncPatternSource(
        num_allocations=64, num_threads=4, pattern="independent", speed_skew=4
    )
    allocations = source.get_allocations()
    lane_maxima = [max(a.end[lane] for a in allocations) for lane in range(4)]
    assert len(set(lane_maxima)) > 1


def test_speed_skew_keeps_every_lifetime_non_empty() -> None:
    for skew in (1, 2, 3, 5):
        source = SyncPatternSource(
            num_allocations=128, num_threads=8, pattern="barrier", speed_skew=skew
        )
        allocations = source.get_allocations()
        assert all(a.start != a.end for a in allocations), skew


def test_speed_skew_of_one_leaves_the_instance_unchanged() -> None:
    plain = SyncPatternSource(num_allocations=48, num_threads=4, seed=5)
    skewed = SyncPatternSource(num_allocations=48, num_threads=4, speed_skew=1, seed=5)
    assert plain.get_allocations() == skewed.get_allocations()


def test_size_distribution_reaches_the_allocations() -> None:
    flat = SyncPatternSource(num_allocations=200, size_distribution="uniform")
    dominant = SyncPatternSource(num_allocations=200, size_distribution="dominant")
    largest_flat = max(a.size for a in flat.get_allocations())
    sizes = sorted((a.size for a in dominant.get_allocations()), reverse=True)
    assert sizes[0] > 10 * sizes[1]
    assert largest_flat > 0


def test_label_carries_thread_count_and_topology() -> None:
    source = SyncPatternSource(num_threads=16, pattern="tree")
    assert source.label() == "sync_pattern[num_threads=16,pattern=tree]"
    assert SyncPatternSource().label() == "sync_pattern"


def test_label_differs_between_thread_counts() -> None:
    assert SyncPatternSource(num_threads=4).label() != (
        SyncPatternSource(num_threads=16).label()
    )


def test_unknown_size_distribution_rejected() -> None:
    with pytest.raises(ValueError, match="not a valid SizeDistribution"):
        SyncPatternSource(size_distribution="normal")


def test_non_positive_speed_skew_rejected() -> None:
    with pytest.raises(ValueError, match="speed_skew must be positive"):
        SyncPatternSource(speed_skew=0)


def test_every_step_barrier_linearizes_while_a_sparser_one_does_not() -> None:
    tight = SyncPatternSource(
        num_allocations=128, num_threads=8, pattern="barrier", sync_period=1, seed=1
    ).get_allocations()
    loose = SyncPatternSource(
        num_allocations=128, num_threads=8, pattern="pairs", sync_period=16, seed=1
    ).get_allocations()
    assert try_linearize(tight, work_budget=None) is not None
    assert try_linearize(loose, work_budget=None) is None
