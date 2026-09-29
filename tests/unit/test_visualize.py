#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest

pytest.importorskip("matplotlib")

from omnimalloc import visualize
from omnimalloc.primitives import Allocation, AllocationKind, Memory, Pool, System
from omnimalloc.visualize import (
    _byte_unit,
    _conflict_pairs,
    _conflict_visibility,
    _format_bytes,
    _lane_panels,
    _panel_extents,
    _projection_panels,
    _select_lanes,
    _y_limits,
    plot_allocation,
)


def _placed(
    i: int, size: int = 100, start: object = 0, end: object = 10, **kw: object
) -> Allocation:
    return Allocation(id=i, size=size, start=start, end=end, offset=0, **kw)


def _pool(pool_id: object, *allocations: Allocation, offset: int = 0) -> Pool:
    return Pool(id=pool_id, allocations=allocations, offset=offset)


KINDS = tuple(
    _placed(i, start=5 * i, end=5 * i + 5, kind=kind)
    for i, kind in enumerate(AllocationKind)
)
ONE_POOL = _pool(1, _placed(1))


def _concurrent_memory() -> Memory:
    alloc1 = Allocation(id=1, size=10, start=(0, 0), end=(1, 0), offset=0)
    alloc2 = Allocation(id=2, size=10, start=(2, 0), end=(3, 0), offset=0)
    alloc3 = Allocation(id=3, size=10, start=(0, 0), end=(0, 1), offset=10)
    alloc4 = Allocation(id=4, size=10, start=(0, 2), end=(0, 3), offset=10)
    return Memory(
        id="mem", pools=(Pool(id=1, allocations=(alloc1, alloc2, alloc3, alloc4)),)
    )


PLOTS = {
    "pool": (ONE_POOL, {}),
    "kinds": (_pool(1, *KINDS), {}),
    "raw_allocations": (KINDS, {}),
    "empty_pool": (_pool("empty"), {}),
    "memory": (
        Memory(id="m", pools=(ONE_POOL, _pool(2, _placed(2), offset=200)), size=500),
        {},
    ),
    "memory_without_size": (Memory(id="m", pools=(ONE_POOL,)), {}),
    "memory_with_an_empty_pool": (
        Memory(id="m", pools=(_pool("empty"), _pool("full", _placed(1)))),
        {},
    ),
    "system": (
        System(
            id="s",
            memories=(
                Memory(id="a", pools=(ONE_POOL,), size=500),
                Memory(id="b", pools=(_pool("x", _placed("y", start=5)),), size=500),
            ),
        ),
        {},
    ),
    "memory_capacities": (
        System(id="s", memories=(Memory(id="m", pools=(ONE_POOL,), size=1000),)),
        {"capacities": {"m": {"budget": 200, "threshold": 220}, "other": {"x": 9}}},
    ),
    "pool_capacities": (_pool("p", _placed(1)), {"capacities": {"p": {"budget": 200}}}),
    "mixed_dimension_pools": (
        Memory(
            id="m",
            pools=(
                ONE_POOL,
                _pool(2, _placed(2, start=(0, 1), end=(2, 3)), offset=200),
            ),
            size=1000,
        ),
        {},
    ),
    "vector_panel": (
        _pool(
            1,
            _placed(1, start=(0, 0), end=(3, 0)).with_offset(100),
            _placed(2, start=(0, 0), end=(0, 3)),
        ),
        {},
    ),
    "hidden_conflicts_panel": (
        Memory(id="m", pools=(_pool(1, *_concurrent_memory().pools[0].allocations),)),
        {},
    ),
    "vector_lanes": (
        _pool(
            1,
            _placed(1, start=(0, 1), end=(2, 3)),
            _placed(2, start=(2, 3), end=(4, 5)),
            _placed(3, size=50, start=(1, 0), end=(3, 4)).with_offset(100),
        ),
        {"view": "lanes"},
    ),
}


@pytest.mark.parametrize(("entity", "kwargs"), PLOTS.values(), ids=PLOTS.keys())
def test_plot_allocation_writes_a_pdf(
    entity: object, kwargs: dict[str, object], tmp_path: Path
) -> None:
    path = tmp_path / "plot.pdf"
    plot_allocation(entity, path, **kwargs)
    assert path.read_bytes().startswith(b"%PDF-")


def test_plot_allocation_writes_a_png_for_a_png_path(tmp_path: Path) -> None:
    path = tmp_path / "plot.png"
    plot_allocation(ONE_POOL, path)
    assert path.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")


UNPLACED = Allocation(id=1, size=10, start=0, end=5)
EMPTY_SYSTEM = System(id="empty", memories=())
UNPLACED_BASE = Memory(id="m", pools=(Pool(id="p", allocations=(_placed(1),)),))


@pytest.mark.parametrize(
    ("entity", "kwargs", "match"),
    [
        (ONE_POOL, {"view": "spiral"}, "view"),
        (ONE_POOL, {"max_lanes": 0}, "max_lanes"),
        (ONE_POOL, {"max_lanes": 2}, "max_lanes"),
        (EMPTY_SYSTEM, {}, "no memories"),
        (EMPTY_SYSTEM, {"view": "lanes"}, "no memories"),
        (UNPLACED_BASE, {}, "offsets"),
        (Pool(id="p", allocations=(UNPLACED,)), {}, "offsets"),
    ],
)
def test_plot_allocation_rejects(
    entity: object, kwargs: dict[str, object], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        plot_allocation(entity, **kwargs)


def test_byte_unit_picks_largest_unit_that_keeps_value_above_one() -> None:
    assert _byte_unit(500) == (1, "B")
    assert _byte_unit(1024) == (1024, "KB")
    assert _byte_unit(1024**2) == (1024**2, "MB")
    assert _byte_unit(1024**3) == (1024**3, "GB")


def test_format_bytes_does_not_collapse_small_values_to_zero() -> None:
    assert _format_bytes(3000) == "2.9KB"
    assert _format_bytes(200) == "200.0B"


def test_plot_allocation_without_path_shows_figure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import matplotlib.pyplot as plt

    shown = []
    monkeypatch.setattr(plt, "show", lambda: shown.append(True))
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    plot_allocation(Pool(id=1, allocations=(alloc,), offset=0))
    assert shown == [True]


def test_panel_extents_scalar_memory_is_identity_and_exact() -> None:
    alloc1 = Allocation(id=1, size=10, start=2, end=7, offset=0)
    alloc2 = Allocation(id=2, size=10, start=5, end=9, offset=10)
    memory = Memory(id="mem", pools=(Pool(id=1, allocations=(alloc1, alloc2)),))

    extents, exact = _panel_extents(memory)

    assert exact
    assert extents[id(alloc1)] == (2, 7)
    assert extents[id(alloc2)] == (5, 9)


def test_panel_extents_linearizable_vector_memory_is_exact() -> None:
    alloc1 = Allocation(id=1, size=10, start=(0, 0), end=(1, 1), offset=0)
    alloc2 = Allocation(id=2, size=10, start=(1, 1), end=(2, 2), offset=0)
    alloc3 = Allocation(id=3, size=10, start=(2, 2), end=(3, 3), offset=0)
    memory = Memory(id="mem", pools=(Pool(id=1, allocations=(alloc1, alloc2, alloc3)),))

    extents, exact = _panel_extents(memory)

    assert exact
    starts = [extents[id(a)] for a in (alloc1, alloc2, alloc3)]
    assert starts == sorted(starts)
    assert all(start < end for start, end in starts)


def test_panel_extents_concurrent_vector_memory_falls_back_to_sums() -> None:
    memory = _concurrent_memory()
    alloc1, alloc2, alloc3, alloc4 = memory.pools[0].allocations

    extents, exact = _panel_extents(memory)

    assert not exact
    assert extents[id(alloc1)] == (0, 1)
    assert extents[id(alloc2)] == (2, 3)
    assert extents[id(alloc3)] == (0, 1)
    assert extents[id(alloc4)] == (2, 3)


@pytest.mark.parametrize(
    ("intervals", "expected"),
    [
        ([(0, 2), (2, 4), (4, 6)], 0),
        ([(0, 3), (1, 4), (2, 5)], 3),
        ([(0, 10), (1, 2), (3, 4)], 2),
    ],
)
def test_conflict_pairs_ignores_touching_intervals(
    intervals: list[tuple[int, int]], expected: int
) -> None:
    allocations = [
        Allocation(id=i, size=1, start=start, end=end)
        for i, (start, end) in enumerate(intervals)
    ]
    assert _conflict_pairs(allocations) == expected


def test_conflict_visibility_counts_hidden_conflicts() -> None:
    memory = _concurrent_memory()
    extents, _ = _panel_extents(memory)

    visible, total = _conflict_visibility(memory, extents)

    assert total == 4
    assert visible == 2


def test_projection_panels_note_reports_conflict_visibility() -> None:
    system = System(id="sys", memories=(_concurrent_memory(),))

    panels, caveat = _projection_panels(system)

    assert panels[0].note == "2/4 conflicts visible"
    assert caveat is not None


def test_projection_panels_skip_conflict_note_over_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = System(id="sys", memories=(_concurrent_memory(),))

    def _over_budget(_allocations: object, **_kwargs: object) -> list[int]:
        raise RuntimeError("Conflict sweep work exceeds work_budget")

    monkeypatch.setattr(visualize, "conflict_degrees", _over_budget)

    panels, caveat = _projection_panels(system)

    assert panels[0].note is None
    assert caveat is not None


def test_projection_panels_use_per_memory_x_limits() -> None:
    short_mem = Memory(
        id="short",
        pools=(Pool(id=1, allocations=(Allocation(id=1, size=10, start=0, end=4),)),),
    )
    long_mem = Memory(
        id="long",
        pools=(
            Pool(id=2, allocations=(Allocation(id=2, size=10, start=0, end=100_000),)),
        ),
    )
    system = System(id="sys", memories=(short_mem, long_mem))

    panels, _ = _projection_panels(system)

    assert panels[0].x_limits == (0, 4)
    assert panels[1].x_limits == (0, 100_000)


def test_lane_panels_use_per_lane_x_limits() -> None:
    alloc = Allocation(id=1, size=10, start=(0, 0), end=(3, 7), offset=0)
    memory = Memory(id="mem", pools=(Pool(id=1, allocations=(alloc,)),))
    system = System(id="sys", memories=(memory,))

    panels, _ = _lane_panels(system, max_lanes=None)

    assert panels[0].x_limits == (0, 3)
    assert panels[1].x_limits == (0, 7)


def test_lane_panels_x_limits_stay_independent_across_memories() -> None:
    long_alloc = Allocation(id=1, size=10, start=(0, 0), end=(100, 5), offset=0)
    short_alloc = Allocation(id=2, size=10, start=(0, 0), end=(10, 5), offset=0)
    system = System(
        id="sys",
        memories=(
            Memory(id="a", pools=(Pool(id=1, allocations=(long_alloc,)),)),
            Memory(id="b", pools=(Pool(id=2, allocations=(short_alloc,)),)),
        ),
    )

    panels, _ = _lane_panels(system, max_lanes=None)

    assert [panel.x_limits for panel in panels] == [
        (0, 100),
        (0, 5),
        (0, 10),
        (0, 5),
    ]


def test_select_lanes_keeps_all_lanes_when_under_cap() -> None:
    allocations = [Allocation(id=1, size=10, start=(0, 0), end=(1, 1), offset=0)]

    assert _select_lanes(allocations, 2, None) == [0, 1]
    assert _select_lanes(allocations, 2, 2) == [0, 1]
    assert _select_lanes(allocations, 2, 5) == [0, 1]


def test_select_lanes_picks_top_k_by_definite_peak() -> None:
    allocations = [
        Allocation(id=1, size=100, start=(0, 0), end=(4, 0), offset=0),
        Allocation(id=2, size=60, start=(0, 0), end=(0, 4), offset=100),
        Allocation(id=3, size=60, start=(0, 1), end=(0, 3), offset=160),
    ]

    assert _select_lanes(allocations, 2, 1) == [1]


def test_lane_panels_titles_report_truncation() -> None:
    alloc1 = Allocation(id=1, size=100, start=(0, 0), end=(4, 0), offset=0)
    alloc2 = Allocation(id=2, size=60, start=(0, 0), end=(0, 4), offset=100)
    alloc3 = Allocation(id=3, size=60, start=(0, 1), end=(0, 3), offset=160)
    pool = Pool(id=1, allocations=(alloc1, alloc2, alloc3), offset=0)
    system = System(id="sys", memories=(Memory(id="mem", pools=(pool,)),))

    panels, caveat = _lane_panels(system, max_lanes=1)

    assert len(panels) == 1
    assert panels[0].title is not None
    assert "top 1 of 2 threads" in panels[0].title
    assert panels[0].xlabel == "Thread 1 Time (Step)"
    assert caveat is not None


def test_panel_projection_never_shows_false_conflicts() -> None:
    rng_starts = [(i, 0) if i % 2 == 0 else (0, i) for i in range(1, 9)]
    allocations = tuple(
        Allocation(
            id=i,
            size=10 + i,
            start=start,
            end=(start[0] + 2, start[1]) if start[1] == 0 else (0, start[1] + 2),
            offset=20 * i,
        )
        for i, start in enumerate(rng_starts)
    )
    memory = Memory(id="mem", pools=(Pool(id=1, allocations=allocations, offset=0),))

    extents, exact = _panel_extents(memory)

    assert not exact
    for a in allocations:
        for b in allocations:
            if a.id >= b.id:
                continue
            (sa, ea), (sb, eb) = extents[id(a)], extents[id(b)]
            if sa < eb and sb < ea:
                assert a.conflicts_with(b)


def test_y_limits_cover_a_pool_pinned_above_the_sum_of_pool_sizes() -> None:
    high = Pool(
        id="high",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),),
        offset=1000,
    )
    low = Pool(
        id="low",
        allocations=(Allocation(id=2, size=10, start=0, end=5, offset=0),),
        offset=0,
    )
    _, upper = _y_limits(Memory(id="m", pools=(high, low)))
    assert upper >= 1010
