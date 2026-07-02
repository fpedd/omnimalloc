#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc import run_allocation, validate_allocation
from omnimalloc.benchmark.sources import BaseSource
from omnimalloc.benchmark.sources.pinwheel import PinwheelSource
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.primitives.utils import get_pressure


def _signatures(allocations: tuple[Allocation, ...]) -> list[tuple[int, int, int]]:
    return [(a.start, a.end, a.size) for a in allocations]


def _has_guillotine_cut(pool: Pool) -> bool:
    allocs = pool.allocations
    t_lo, t_hi = min(a.start for a in allocs), max(a.end for a in allocs)
    m_lo, m_hi = min(a.offset for a in allocs), max(a.offset + a.size for a in allocs)
    times = {a.start for a in allocs} | {a.end for a in allocs}
    mems = {a.offset for a in allocs} | {a.offset + a.size for a in allocs}
    for t in times:
        if t_lo < t < t_hi and not any(a.start < t < a.end for a in allocs):
            return True
    for m in mems:
        if m_lo < m < m_hi and not any(
            a.offset < m < a.offset + a.size for a in allocs
        ):
            return True
    return False


def test_pinwheel_source_is_registered() -> None:
    assert "pinwheel_source" in BaseSource.registry()
    assert BaseSource.get("pinwheel_source") is PinwheelSource


def test_pinwheel_count_rounds_up_to_pinwheel_size() -> None:
    allocations = PinwheelSource(num_allocations=64).get_allocations()
    assert len(allocations) == 65
    assert (len(allocations) - 1) % 4 == 0


@pytest.mark.parametrize("num", [5, 17, 65, 257, 513])
def test_pinwheel_optimum_is_tight(num: int) -> None:
    capacity = 1024 * 1024
    source = PinwheelSource(num_allocations=num, capacity=capacity)
    allocations = source.get_allocations()
    assert get_pressure(allocations) == capacity


def test_pinwheel_allocations_fit_within_makespan() -> None:
    makespan = 4096
    source = PinwheelSource(num_allocations=64, makespan=makespan, min_size=1)
    for alloc in source.get_allocations():
        assert 0 <= alloc.start < alloc.end <= makespan


def test_pinwheel_respects_min_size() -> None:
    source = PinwheelSource(num_allocations=256, min_size=2048)
    assert all(a.size >= 2048 for a in source.get_allocations())


def test_pinwheel_is_deterministic_per_seed() -> None:
    a = PinwheelSource(num_allocations=129, seed=7).get_allocations()
    b = PinwheelSource(num_allocations=129, seed=7).get_allocations()
    c = PinwheelSource(num_allocations=129, seed=8).get_allocations()
    assert _signatures(a) == _signatures(b)
    assert _signatures(a) != _signatures(c)


def test_pinwheel_distinct_pools_differ() -> None:
    source = PinwheelSource(num_allocations=33)
    pools = source.get_pools(num_pools=2)
    assert len(pools) == 2
    assert _signatures(pools[0].allocations) != _signatures(pools[1].allocations)


def test_pinwheel_rejects_capacity_too_small() -> None:
    with pytest.raises(ValueError, match="capacity"):
        PinwheelSource(capacity=2048, min_size=1024)


def test_pinwheel_rejects_makespan_too_small() -> None:
    with pytest.raises(ValueError, match="makespan"):
        PinwheelSource(makespan=2, min_duration=1)


def test_pinwheel_rejects_nonpositive_min_size() -> None:
    with pytest.raises(ValueError, match="min_size"):
        PinwheelSource(min_size=0)


def test_pinwheel_ground_truth_is_valid_and_optimal() -> None:
    capacity = 1024 * 1024
    source = PinwheelSource(num_allocations=200, capacity=capacity)
    pool = source.get_ground_truth_pool()

    validate_allocation(pool)
    assert pool.is_allocated
    assert pool.size == capacity
    assert pool.pressure == capacity


def test_pinwheel_ground_truth_matches_get_allocations() -> None:
    source = PinwheelSource(num_allocations=64)
    truth = source.get_ground_truth_pool()
    allocs = source.get_allocations()
    assert _signatures(truth.allocations) == _signatures(allocs)


def test_pinwheel_packing_is_non_guillotine() -> None:
    pool = PinwheelSource(num_allocations=64).get_ground_truth_pool()
    assert not _has_guillotine_cut(pool)


def test_pinwheel_no_allocator_beats_the_optimum() -> None:
    capacity = 1024 * 1024
    source = PinwheelSource(num_allocations=150, capacity=capacity)
    pool = source.get_pool()
    allocated = run_allocation(pool, "greedy_by_size_allocator_cpp", validate=True)
    assert allocated.size >= capacity
