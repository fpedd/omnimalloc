#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc.allocate import run_allocation
from omnimalloc.allocators.greedy import (
    GreedyAllocator,
    GreedyByAreaAllocator,
    GreedyByConflictAllocator,
    GreedyByDurationAllocator,
    GreedyBySizeAllocator,
)
from omnimalloc.benchmark.sources.generator import (
    HighContentionSource,
    PowerOf2Source,
    RandomSource,
    SequentialSource,
    UniformSource,
)
from omnimalloc.primitives.memory import Memory
from omnimalloc.primitives.pool import Pool
from omnimalloc.validate import validate_allocation
from omnimalloc.visualize import HAS_MATPLOTLIB, plot_allocation


def test_greedy_with_random_source() -> None:
    source = RandomSource(num_allocations=50, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocator = GreedyAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert validate_allocation(allocated_pool)
    assert all(a.offset is not None for a in allocated_pool.allocations)


def test_greedy_by_size_with_random_source() -> None:
    source = RandomSource(num_allocations=50, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocator = GreedyBySizeAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert validate_allocation(allocated_pool)
    assert all(a.offset is not None for a in allocated_pool.allocations)


def test_greedy_by_duration_with_sequential_source() -> None:
    source = SequentialSource(num_allocations=30, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocator = GreedyByDurationAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert validate_allocation(allocated_pool)
    assert allocated_pool.size > 0


def test_greedy_by_conflict_with_high_contention() -> None:
    source = HighContentionSource(num_allocations=40, time_window=10, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocator = GreedyByConflictAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert validate_allocation(allocated_pool)
    assert all(a.offset is not None for a in allocated_pool.allocations)


def test_greedy_by_area_with_power_of_2_source() -> None:
    source = PowerOf2Source(num_allocations=25, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocator = GreedyByAreaAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert validate_allocation(allocated_pool)
    assert allocated_pool.size > 0


def test_greedy_with_uniform_source() -> None:
    source = UniformSource(num_allocations=20, size=1024, duration=5, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocator = GreedyAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert validate_allocation(allocated_pool)
    assert all(a.size == 1024 for a in allocated_pool.allocations)


def test_greedy_allocators_produce_different_results() -> None:
    source = RandomSource(num_allocations=30, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocators = [
        GreedyAllocator(),
        GreedyBySizeAllocator(),
        GreedyByDurationAllocator(),
        GreedyByConflictAllocator(),
        GreedyByAreaAllocator(),
    ]

    results = []
    for allocator in allocators:
        allocated_pool = run_allocation(pool, allocator)
        assert validate_allocation(allocated_pool)
        results.append(allocated_pool.size)

    assert len(set(results)) > 1


def test_greedy_with_memory_hierarchy() -> None:
    source = RandomSource(num_allocations=40, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)
    memory = Memory(id="test_memory", pools=(pool,))

    allocator = GreedyBySizeAllocator()
    allocated_memory = run_allocation(memory, allocator)

    assert validate_allocation(allocated_memory)
    assert allocated_memory.used_size > 0
    assert len(allocated_memory.pools) == 1


def test_greedy_with_large_workload() -> None:
    source = RandomSource(num_allocations=200, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocator = GreedyAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert validate_allocation(allocated_pool)
    assert len(allocated_pool.allocations) == 200


def test_greedy_by_conflict_minimizes_peak_with_contention() -> None:
    source = HighContentionSource(num_allocations=50, time_window=15, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    conflict_allocator = GreedyByConflictAllocator()
    basic_allocator = GreedyAllocator()

    conflict_pool = run_allocation(pool, conflict_allocator)
    basic_pool = run_allocation(pool, basic_allocator)

    assert validate_allocation(conflict_pool)
    assert validate_allocation(basic_pool)
    assert conflict_pool.size <= basic_pool.size * 1.5


def test_greedy_deterministic_across_runs() -> None:
    source = RandomSource(num_allocations=50, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocator = GreedyBySizeAllocator()

    result1 = run_allocation(pool, allocator)
    result2 = run_allocation(pool, allocator)

    assert result1.size == result2.size
    offsets1 = [a.offset for a in result1.allocations]
    offsets2 = [a.offset for a in result2.allocations]
    assert offsets1 == offsets2


def test_greedy_with_sequential_produces_small_footprint() -> None:
    source = SequentialSource(num_allocations=100, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocator = GreedyAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert validate_allocation(allocated_pool)
    total_alloc_size = sum(a.size for a in allocations)
    avg_alloc_size = total_alloc_size // len(allocations)
    assert allocated_pool.size < avg_alloc_size * 10


def test_greedy_by_area_with_varying_durations() -> None:
    source = RandomSource(
        num_allocations=30,
        duration_min=1,
        duration_max=50,
        size_min=1024,
        size_max=10240,
        seed=42,
    )
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocator = GreedyByAreaAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert validate_allocation(allocated_pool)
    assert allocated_pool.size > 0


def test_all_greedy_variants_handle_empty_pool() -> None:
    pool = Pool(id="empty_pool", allocations=())

    allocators = [
        GreedyAllocator(),
        GreedyBySizeAllocator(),
        GreedyByDurationAllocator(),
        GreedyByConflictAllocator(),
        GreedyByAreaAllocator(),
    ]

    for allocator in allocators:
        allocated_pool = run_allocation(pool, allocator)
        assert validate_allocation(allocated_pool)
        assert len(allocated_pool.allocations) == 0
        assert allocated_pool.size == 0


@pytest.mark.skipif(not HAS_MATPLOTLIB, reason="matplotlib not installed")
def test_greedy_allocators_with_artifacts(artifacts_dir: Path) -> None:
    source = RandomSource(num_allocations=30, seed=42)
    allocations = source.get_allocations()
    pool = Pool(id="test_pool", allocations=allocations)

    allocators = {
        "greedy": GreedyAllocator(),
        "greedy_by_size": GreedyBySizeAllocator(),
        "greedy_by_duration": GreedyByDurationAllocator(),
        "greedy_by_conflict": GreedyByConflictAllocator(),
        "greedy_by_area": GreedyByAreaAllocator(),
    }

    for name, allocator in allocators.items():
        allocated_pool = run_allocation(pool, allocator)
        assert validate_allocation(allocated_pool)

        output_file = artifacts_dir / f"{name}.pdf"
        plot_allocation(allocated_pool, output_file)
        assert output_file.exists()
        assert output_file.stat().st_size > 0
