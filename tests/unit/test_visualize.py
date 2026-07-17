#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc import visualize
from omnimalloc.primitives import Allocation, AllocationKind, Memory, Pool, System
from omnimalloc.visualize import (
    HAS_MATPLOTLIB,
    _byte_unit,
    _conflict_pairs,
    _conflict_visibility,
    _format_bytes,
    _lane_panels,
    _panel_extents,
    _projection_panels,
    _select_lanes,
    plot_allocation,
)

pytestmark = pytest.mark.skipif(not HAS_MATPLOTLIB, reason="matplotlib not installed")


def test_visualize_single_allocation(artifacts_dir: Path) -> None:
    """Test visualization with a single simple allocation."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)

    output_path = artifacts_dir / "test_single.pdf"
    plot_allocation(pool, output_path)
    assert output_path.exists()
    assert output_path.stat().st_size > 0


def test_visualize_multiple_allocations_in_pool(artifacts_dir: Path) -> None:
    """Test visualization with multiple non-overlapping allocations in a pool."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=150, start=5, end=10, offset=0)
    alloc3 = Allocation(id=3, size=75, start=10, end=15, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2, alloc3), offset=0)

    output_path = artifacts_dir / "test_multiple.pdf"
    plot_allocation(pool, output_path)
    assert output_path.exists()


def test_visualize_with_allocation_kinds(artifacts_dir: Path) -> None:
    """Test visualization with different allocation kinds."""
    alloc1 = Allocation(
        id=1, size=100, start=0, end=5, offset=0, kind=AllocationKind.WORKSPACE
    )
    alloc2 = Allocation(
        id=2, size=150, start=5, end=10, offset=0, kind=AllocationKind.CONSTANT
    )
    alloc3 = Allocation(
        id=3, size=75, start=10, end=15, offset=0, kind=AllocationKind.INPUT
    )
    alloc4 = Allocation(
        id=4, size=50, start=15, end=20, offset=0, kind=AllocationKind.OUTPUT
    )
    pool = Pool(id=1, allocations=(alloc1, alloc2, alloc3, alloc4), offset=0)

    output_path = artifacts_dir / "test_kinds.pdf"
    plot_allocation(pool, output_path)
    assert output_path.exists()


def test_visualize_memory_with_multiple_pools(artifacts_dir: Path) -> None:
    """Test visualization of memory with multiple pools."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=150, start=0, end=10, offset=0)
    alloc3 = Allocation(id=3, size=75, start=5, end=15, offset=0)

    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=200)
    pool3 = Pool(id=3, allocations=(alloc3,), offset=500)

    memory = Memory(id="mem_1", pools=(pool1, pool2, pool3), capacity=1000)

    output_path = artifacts_dir / "test_memory_pools.pdf"
    plot_allocation(memory, output_path)
    assert output_path.exists()


def test_visualize_system_with_multiple_memories(artifacts_dir: Path) -> None:
    """Test visualization of system with multiple memories."""
    # Memory 1: Simple pool
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    memory1 = Memory(id="ddr4_1", pools=(pool1,), capacity=500)

    # Memory 2: Multiple pools
    alloc2 = Allocation(id=2, size=150, start=0, end=10, offset=0)
    alloc3 = Allocation(id=3, size=75, start=5, end=15, offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=0)
    pool3 = Pool(id=3, allocations=(alloc3,), offset=200)
    memory2 = Memory(id="ddr4_2", pools=(pool2, pool3), capacity=1000)

    system = System(id="test_system", memories=(memory1, memory2))

    output_path = artifacts_dir / "test_system.pdf"
    plot_allocation(system, output_path)
    assert output_path.exists()


def test_visualize_complex_hierarchy(artifacts_dir: Path) -> None:
    """Test visualization of a complex system with many allocations."""
    # Create a more realistic scenario with multiple memories and pools
    allocations_mem1_pool1 = [
        Allocation(id=i, size=50 + i * 10, start=i * 2, end=(i + 1) * 2, offset=0)
        for i in range(5)
    ]
    allocations_mem1_pool2 = [
        Allocation(
            id=i + 5,
            size=100 + i * 20,
            start=i * 3,
            end=(i + 1) * 3,
            offset=0,
            kind=AllocationKind.CONSTANT,
        )
        for i in range(3)
    ]

    pool1 = Pool(id="pool_1", allocations=tuple(allocations_mem1_pool1), offset=0)
    pool2 = Pool(id="pool_2", allocations=tuple(allocations_mem1_pool2), offset=300)

    memory1 = Memory(id="main_memory", pools=(pool1, pool2), capacity=2048)

    # Second memory with different allocation patterns
    allocations_mem2 = [
        Allocation(
            id=i + 10,
            size=75,
            start=i,
            end=i + 5,
            offset=0,
            kind=AllocationKind.WORKSPACE if i % 2 == 0 else AllocationKind.OUTPUT,
        )
        for i in range(0, 20, 5)
    ]
    pool3 = Pool(id="pool_3", allocations=tuple(allocations_mem2), offset=0)
    memory2 = Memory(id="cache_memory", pools=(pool3,), capacity=1024)

    system = System(id="complex_system", memories=(memory1, memory2))

    output_path = artifacts_dir / "test_complex.pdf"
    plot_allocation(system, output_path)
    assert output_path.exists()
    # Check file is reasonably sized (not empty, not huge)
    size = output_path.stat().st_size
    assert 1000 < size < 10_000_000


def test_visualize_with_string_ids(artifacts_dir: Path) -> None:
    """Test visualization with string IDs."""
    alloc1 = Allocation(id="workspace_buf", size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id="temp_buf", size=150, start=5, end=10, offset=0)
    pool = Pool(id="tensor_pool", allocations=(alloc1, alloc2), offset=0)
    memory = Memory(id="ddr_ram", pools=(pool,), capacity=512)

    output_path = artifacts_dir / "test_string_ids.pdf"
    plot_allocation(memory, output_path)
    assert output_path.exists()


def test_visualize_memory_without_size(artifacts_dir: Path) -> None:
    """Test visualization of memory without explicit size (uses used_size)."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=150, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=200)
    memory = Memory(id=1, pools=(pool1, pool2))  # No size specified

    output_path = artifacts_dir / "test_no_size.pdf"
    plot_allocation(memory, output_path)
    assert output_path.exists()


def test_visualize_pool_converts_to_system(artifacts_dir: Path) -> None:
    """Test that visualizing a Pool creates appropriate wrapper structures."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)

    output_path = artifacts_dir / "test_pool_wrapper.pdf"
    plot_allocation(pool, output_path)
    assert output_path.exists()


def test_visualize_memory_converts_to_system(artifacts_dir: Path) -> None:
    """Test that visualizing a Memory creates appropriate System wrapper."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)
    memory = Memory(id=1, pools=(pool,), capacity=500)

    output_path = artifacts_dir / "test_memory_wrapper.pdf"
    plot_allocation(memory, output_path)
    assert output_path.exists()


def test_visualize_with_capacities(artifacts_dir: Path) -> None:
    """Test visualization with extra capacity lines."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=150, start=5, end=10, offset=100)
    pool = Pool(id=1, allocations=(alloc1, alloc2), offset=0)
    memory = Memory(id="ddr_mem", pools=(pool,), capacity=1000)
    system = System(id="test_sys", memories=(memory,))

    custom_limits = {
        "budget": {"ddr_mem": 200},
        "threshold": {"ddr_mem": 220},
    }

    output_path = artifacts_dir / "test_memory_limits.pdf"
    plot_allocation(system, output_path, capacities=custom_limits)
    assert output_path.exists()
    assert output_path.stat().st_size > 0


def test_visualize_saves_png_when_path_has_png_extension(artifacts_dir: Path) -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)

    output_path = artifacts_dir / "test_extension.png"
    plot_allocation(pool, output_path)
    with output_path.open("rb") as f:
        assert f.read(8) == b"\x89PNG\r\n\x1a\n"


def test_visualize_still_saves_pdf_by_default(artifacts_dir: Path) -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)

    output_path = artifacts_dir / "test_extension.pdf"
    plot_allocation(pool, output_path)
    with output_path.open("rb") as f:
        assert f.read(5) == b"%PDF-"


def test_byte_unit_picks_largest_unit_that_keeps_value_above_one() -> None:
    assert _byte_unit(500) == (1, "B")
    assert _byte_unit(1024) == (1024, "KB")
    assert _byte_unit(1024**2) == (1024**2, "MB")
    assert _byte_unit(1024**3) == (1024**3, "GB")


def test_format_bytes_does_not_collapse_small_values_to_zero() -> None:
    assert _format_bytes(3000) == "2.9KB"
    assert _format_bytes(200) == "200.0B"


def test_visualize_memory_with_empty_pool(artifacts_dir: Path) -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    memory = Memory(
        id="mem",
        pools=(
            Pool(id="empty", allocations=()),
            Pool(id="full", allocations=(alloc,), offset=0),
        ),
    )
    output_path = artifacts_dir / "test_empty_pool.pdf"
    plot_allocation(memory, output_path)
    assert output_path.exists()


def test_visualize_entity_with_zero_used_memory(artifacts_dir: Path) -> None:
    pool = Pool(id="empty", allocations=())
    output_path = artifacts_dir / "test_zero_used.pdf"
    plot_allocation(pool, output_path)
    assert output_path.exists()


def test_visualize_mixed_dimension_pools(artifacts_dir: Path) -> None:
    scalar_pool = Pool(
        id=1,
        allocations=(Allocation(id=1, size=100, start=0, end=4, offset=0),),
        offset=0,
    )
    vector_pool = Pool(
        id=2,
        allocations=(Allocation(id=2, size=50, start=(0, 1), end=(2, 3), offset=0),),
        offset=200,
    )
    memory = Memory(id="mem", pools=(scalar_pool, vector_pool), capacity=1000)

    output_path = artifacts_dir / "test_mixed_dim_pools.pdf"
    plot_allocation(memory, output_path)
    assert output_path.exists()
    assert output_path.stat().st_size > 0


def test_visualize_empty_pool(artifacts_dir: Path) -> None:
    empty = Pool(id=1, allocations=(), offset=0)
    filled = Pool(
        id=2,
        allocations=(Allocation(id=1, size=100, start=0, end=4, offset=0),),
        offset=100,
    )
    memory = Memory(id="mem", pools=(empty, filled), capacity=500)

    output_path = artifacts_dir / "test_empty_pool.pdf"
    plot_allocation(memory, output_path)
    assert output_path.exists()


def test_visualize_vector_time_lanes(artifacts_dir: Path) -> None:
    alloc1 = Allocation(id=1, size=100, start=(0, 1), end=(2, 3), offset=0)
    alloc2 = Allocation(id=2, size=100, start=(2, 3), end=(4, 5), offset=0)
    alloc3 = Allocation(id=3, size=50, start=(1, 0), end=(3, 4), offset=100)
    pool = Pool(id=1, allocations=(alloc1, alloc2, alloc3), offset=0)

    output_path = artifacts_dir / "test_vector_lanes.pdf"
    plot_allocation(pool, output_path, view="lanes")
    assert output_path.exists()
    assert output_path.stat().st_size > 0


def test_plot_allocation_without_path_shows_figure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import matplotlib.pyplot as plt

    shown = []
    monkeypatch.setattr(plt, "show", lambda: shown.append(True))
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    plot_allocation(Pool(id=1, allocations=(alloc,), offset=0))
    assert shown == [True]


def test_plot_allocation_rejects_unknown_view() -> None:
    pool = Pool(id=1, allocations=(Allocation(id=1, size=1, start=0, end=1),))
    with pytest.raises(ValueError, match="view"):
        plot_allocation(pool, view="spiral")


def test_plot_allocation_rejects_non_positive_max_lanes() -> None:
    pool = Pool(id=1, allocations=(Allocation(id=1, size=1, start=0, end=1),))
    with pytest.raises(ValueError, match="max_lanes"):
        plot_allocation(pool, max_lanes=0)


def test_plot_allocation_rejects_max_lanes_with_panel_view() -> None:
    pool = Pool(id=1, allocations=(Allocation(id=1, size=1, start=0, end=1),))
    with pytest.raises(ValueError, match="max_lanes"):
        plot_allocation(pool, max_lanes=2)


def test_plot_allocation_rejects_empty_system() -> None:
    with pytest.raises(ValueError, match="no memories"):
        plot_allocation(System(id="empty", memories=()))
    with pytest.raises(ValueError, match="no memories"):
        plot_allocation(System(id="empty", memories=()), view="lanes")


def test_visualize_vector_time_panel(artifacts_dir: Path) -> None:
    alloc1 = Allocation(id=1, size=100, start=(0, 0), end=(3, 0), offset=100)
    alloc2 = Allocation(id=2, size=100, start=(0, 0), end=(0, 3), offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2), offset=0)

    output_path = artifacts_dir / "test_vector_panel.pdf"
    plot_allocation(pool, output_path)
    assert output_path.exists()
    assert output_path.stat().st_size > 0


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


def _concurrent_memory() -> Memory:
    alloc1 = Allocation(id=1, size=10, start=(0, 0), end=(1, 0), offset=0)
    alloc2 = Allocation(id=2, size=10, start=(2, 0), end=(3, 0), offset=0)
    alloc3 = Allocation(id=3, size=10, start=(0, 0), end=(0, 1), offset=10)
    alloc4 = Allocation(id=4, size=10, start=(0, 2), end=(0, 3), offset=10)
    return Memory(
        id="mem", pools=(Pool(id=1, allocations=(alloc1, alloc2, alloc3, alloc4)),)
    )


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

    def _over_budget(_allocations: object) -> list[int]:
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


def test_panel_projection_never_shows_false_conflicts(artifacts_dir: Path) -> None:
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
    memory = Memory(id="mem", pools=(Pool(id=1, allocations=allocations),))

    extents, exact = _panel_extents(memory)

    assert not exact
    for a in allocations:
        for b in allocations:
            if a.id >= b.id:
                continue
            (sa, ea), (sb, eb) = extents[id(a)], extents[id(b)]
            if sa < eb and sb < ea:
                assert a.conflicts_with(b)

    output_path = artifacts_dir / "test_panel_soundness.pdf"
    plot_allocation(memory, output_path)
    assert output_path.exists()
