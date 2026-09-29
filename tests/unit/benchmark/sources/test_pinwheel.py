#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc import validate_allocation
from omnimalloc.analysis import antichain_pressure
from omnimalloc.benchmark.sources.pinwheel import PinwheelSource
from omnimalloc.primitives import Allocation, Pool


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


def test_pinwheel_rejects_unreachable_count() -> None:
    with pytest.raises(ValueError, match="cannot reach exactly 64"):
        PinwheelSource(num_allocations=64).get_allocations()


@pytest.mark.parametrize("num", [5, 17, 65, 257, 513])
def test_pinwheel_optimum_is_tight(num: int) -> None:
    capacity = 1024 * 1024
    source = PinwheelSource(num_allocations=num, capacity=capacity)
    allocations = source.get_allocations()
    assert antichain_pressure(allocations) == capacity


def test_pinwheel_allocations_fit_within_makespan() -> None:
    makespan = 4096
    source = PinwheelSource(num_allocations=65, makespan=makespan, size_min=1)
    for alloc in source.get_allocations():
        assert 0 <= alloc.start < alloc.end <= makespan


def test_pinwheel_respects_min_size() -> None:
    source = PinwheelSource(num_allocations=257, size_min=2048)
    assert all(a.size >= 2048 for a in source.get_allocations())


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        pytest.param(
            {"capacity": 2048, "size_min": 1024}, "capacity", id="capacity_too_small"
        ),
        pytest.param(
            {"makespan": 2, "duration_min": 1}, "makespan", id="makespan_too_small"
        ),
        pytest.param({"size_min": 0}, "size_min", id="nonpositive_min_size"),
    ],
)
def test_pinwheel_rejects(kwargs: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        PinwheelSource(**kwargs)


def test_pinwheel_ground_truth_is_valid_and_optimal() -> None:
    capacity = 1024 * 1024
    source = PinwheelSource(num_allocations=201, capacity=capacity)
    pool = source.get_ground_truth_pool()

    validate_allocation(pool)
    assert pool.is_allocated
    assert pool.size == capacity
    assert pool.pressure == capacity


def test_pinwheel_ground_truth_matches_get_allocations() -> None:
    source = PinwheelSource(num_allocations=65)
    truth = source.get_ground_truth_pool()
    allocs = source.get_allocations()
    assert _signatures(truth.allocations) == _signatures(allocs)


def test_pinwheel_packing_is_non_guillotine() -> None:
    pool = PinwheelSource(num_allocations=65).get_ground_truth_pool()
    assert not _has_guillotine_cut(pool)
