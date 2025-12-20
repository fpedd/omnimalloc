#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc.primitives import Allocation, BufferKind, Memory, Pool, System
from omnimalloc.visualize import HAS_MATPLOTLIB, _canonicalize, plot_allocation

pytestmark = pytest.mark.skipif(not HAS_MATPLOTLIB, reason="matplotlib not installed")


def test_visualize_single_allocation(artifacts_dir: Path) -> None:
    """Test visualization with a single simple allocation."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)

    output_path = artifacts_dir / "test_single.pdf"
    result = plot_allocation(pool, file_path=output_path)
    assert result == output_path
    assert output_path.exists()
    assert output_path.stat().st_size > 0


def test_visualize_multiple_allocations_in_pool(artifacts_dir: Path) -> None:
    """Test visualization with multiple non-overlapping allocations in a pool."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=150, start=5, end=10, offset=0)
    alloc3 = Allocation(id=3, size=75, start=10, end=15, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2, alloc3), offset=0)

    output_path = artifacts_dir / "test_multiple.pdf"
    result = plot_allocation(pool, file_path=output_path)
    assert result == output_path
    assert output_path.exists()


def test_visualize_with_buffer_kinds(artifacts_dir: Path) -> None:
    """Test visualization with different buffer kinds."""
    alloc1 = Allocation(
        id=1, size=100, start=0, end=5, offset=0, kind=BufferKind.WORKSPACE
    )
    alloc2 = Allocation(
        id=2, size=150, start=5, end=10, offset=0, kind=BufferKind.CONSTANT
    )
    alloc3 = Allocation(
        id=3, size=75, start=10, end=15, offset=0, kind=BufferKind.INPUT
    )
    alloc4 = Allocation(
        id=4, size=50, start=15, end=20, offset=0, kind=BufferKind.OUTPUT
    )
    pool = Pool(id=1, allocations=(alloc1, alloc2, alloc3, alloc4), offset=0)

    output_path = artifacts_dir / "test_kinds.pdf"
    result = plot_allocation(pool, file_path=output_path)
    assert result == output_path
    assert output_path.exists()


def test_visualize_memory_with_multiple_pools(artifacts_dir: Path) -> None:
    """Test visualization of memory with multiple pools."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=150, start=0, end=10, offset=0)
    alloc3 = Allocation(id=3, size=75, start=5, end=15, offset=0)

    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=200)
    pool3 = Pool(id=3, allocations=(alloc3,), offset=500)

    memory = Memory(id="mem_1", pools=(pool1, pool2, pool3), size=1000)

    output_path = artifacts_dir / "test_memory_pools.pdf"
    result = plot_allocation(memory, file_path=output_path)
    assert result == output_path
    assert output_path.exists()


def test_visualize_system_with_multiple_memories(artifacts_dir: Path) -> None:
    """Test visualization of system with multiple memories."""
    # Memory 1: Simple pool
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    memory1 = Memory(id="ddr4_1", pools=(pool1,), size=500)

    # Memory 2: Multiple pools
    alloc2 = Allocation(id=2, size=150, start=0, end=10, offset=0)
    alloc3 = Allocation(id=3, size=75, start=5, end=15, offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=0)
    pool3 = Pool(id=3, allocations=(alloc3,), offset=200)
    memory2 = Memory(id="ddr4_2", pools=(pool2, pool3), size=1000)

    system = System(id="test_system", memories=(memory1, memory2))

    output_path = artifacts_dir / "test_system.pdf"
    result = plot_allocation(system, file_path=output_path)
    assert result == output_path
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
            kind=BufferKind.CONSTANT,
        )
        for i in range(3)
    ]

    pool1 = Pool(id="pool_1", allocations=tuple(allocations_mem1_pool1), offset=0)
    pool2 = Pool(id="pool_2", allocations=tuple(allocations_mem1_pool2), offset=300)

    memory1 = Memory(id="main_memory", pools=(pool1, pool2), size=2048)

    # Second memory with different allocation patterns
    allocations_mem2 = [
        Allocation(
            id=i + 10,
            size=75,
            start=i,
            end=i + 5,
            offset=0,
            kind=BufferKind.WORKSPACE if i % 2 == 0 else BufferKind.OUTPUT,
        )
        for i in range(0, 20, 5)
    ]
    pool3 = Pool(id="pool_3", allocations=tuple(allocations_mem2), offset=0)
    memory2 = Memory(id="cache_memory", pools=(pool3,), size=1024)

    system = System(id="complex_system", memories=(memory1, memory2))

    output_path = artifacts_dir / "test_complex.pdf"
    result = plot_allocation(system, file_path=output_path)
    assert result == output_path
    assert output_path.exists()
    # Check file is reasonably sized (not empty, not huge)
    size = output_path.stat().st_size
    assert 1000 < size < 10_000_000


def test_visualize_with_string_ids(artifacts_dir: Path) -> None:
    """Test visualization with string IDs."""
    alloc1 = Allocation(id="workspace_buf", size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id="temp_buf", size=150, start=5, end=10, offset=0)
    pool = Pool(id="tensor_pool", allocations=(alloc1, alloc2), offset=0)
    memory = Memory(id="ddr_ram", pools=(pool,), size=512)

    output_path = artifacts_dir / "test_string_ids.pdf"
    result = plot_allocation(memory, file_path=output_path)
    assert result == output_path
    assert output_path.exists()


def test_visualize_memory_without_size(artifacts_dir: Path) -> None:
    """Test visualization of memory without explicit size (uses used_size)."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=150, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=200)
    memory = Memory(id=1, pools=(pool1, pool2))  # No size specified

    output_path = artifacts_dir / "test_no_size.pdf"
    result = plot_allocation(memory, file_path=output_path)
    assert result == output_path
    assert output_path.exists()


def test_canonicalize_assigns_sequential_ids() -> None:
    """Test that canonicalize assigns sequential IDs to allocations."""
    alloc1 = Allocation(id="z", size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id="a", size=150, start=5, end=10, offset=0)
    alloc3 = Allocation(id="m", size=75, start=10, end=15, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2, alloc3), offset=0)
    memory = Memory(id=1, pools=(pool,))
    system = System(id=1, memories=(memory,))

    canonical = _canonicalize(system)

    # Check that allocations are sorted and have sequential IDs
    canonical_pool = canonical.memories[0].pools[0]
    alloc_ids = [alloc.id for alloc in canonical_pool.allocations]
    assert alloc_ids == [0, 1, 2]  # Sequential starting from 0


def test_canonicalize_sorts_by_start_time() -> None:
    """Test that canonicalize sorts allocations by start time."""
    alloc1 = Allocation(id=1, size=100, start=10, end=15, offset=0)
    alloc2 = Allocation(id=2, size=150, start=0, end=5, offset=0)
    alloc3 = Allocation(id=3, size=75, start=5, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2, alloc3), offset=0)
    memory = Memory(id=1, pools=(pool,))
    system = System(id=1, memories=(memory,))

    canonical = _canonicalize(system)

    canonical_pool = canonical.memories[0].pools[0]
    starts = [alloc.start for alloc in canonical_pool.allocations]
    assert starts == [0, 5, 10]  # Sorted by start time


def test_canonicalize_preserves_allocation_properties() -> None:
    """Test that canonicalize preserves allocation properties except ID."""
    alloc = Allocation(
        id="original", size=100, start=5, end=15, offset=50, kind=BufferKind.CONSTANT
    )
    pool = Pool(id="pool", allocations=(alloc,), offset=100)
    memory = Memory(id="mem", pools=(pool,), size=1000)
    system = System(id="sys", memories=(memory,))

    canonical = _canonicalize(system)

    canonical_alloc = canonical.memories[0].pools[0].allocations[0]
    assert canonical_alloc.id == 0  # Changed to sequential
    assert canonical_alloc.size == 100
    assert canonical_alloc.start == 5
    assert canonical_alloc.end == 15
    assert canonical_alloc.offset == 50
    assert canonical_alloc.kind == BufferKind.CONSTANT


def test_visualize_pool_converts_to_system(artifacts_dir: Path) -> None:
    """Test that visualizing a Pool creates appropriate wrapper structures."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)

    output_path = artifacts_dir / "test_pool_wrapper.pdf"
    result = plot_allocation(pool, file_path=output_path)
    assert result == output_path
    assert output_path.exists()


def test_visualize_memory_converts_to_system(artifacts_dir: Path) -> None:
    """Test that visualizing a Memory creates appropriate System wrapper."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)
    memory = Memory(id=1, pools=(pool,), size=500)

    output_path = artifacts_dir / "test_memory_wrapper.pdf"
    result = plot_allocation(memory, file_path=output_path)
    assert result == output_path
    assert output_path.exists()


def test_visualize_with_memory_limits(artifacts_dir: Path) -> None:
    """Test visualization with custom memory limits."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=150, start=5, end=10, offset=100)
    pool = Pool(id=1, allocations=(alloc1, alloc2), offset=0)
    memory = Memory(id="ddr_mem", pools=(pool,), size=1000)
    system = System(id="test_sys", memories=(memory,))

    custom_limits = {
        "budget": {"ddr_mem": 200},
        "threshold": {"ddr_mem": 220},
    }

    output_path = artifacts_dir / "test_memory_limits.pdf"
    result = plot_allocation(system, file_path=output_path, memory_limits=custom_limits)
    assert result == output_path
    assert output_path.exists()
    assert output_path.stat().st_size > 0
