#
# SPDX-License-Identifier: Apache-2.0
#


import tempfile
from pathlib import Path
from typing import Any

import pytest
from omnimalloc import run_allocation
from omnimalloc.allocators import GreedyAllocator
from omnimalloc.benchmark.results.result import BenchmarkResult
from omnimalloc.benchmark.sources.generator import RandomSource


@pytest.fixture  # type: ignore[misc]
def allocated_pool() -> tuple[Any, GreedyAllocator, RandomSource]:
    """Create an allocated pool for testing."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = source.get_pool()
    allocated_pool = run_allocation(pool, allocator)
    return allocated_pool, allocator, source


def test_benchmark_result_creation_basic(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test basic BenchmarkResult creation."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert result.id == 0
    assert result.duration == 0.5
    assert result.entity.is_allocated


def test_benchmark_result_id_int(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test BenchmarkResult with integer ID."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=42,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert result.id == 42


def test_benchmark_result_id_string(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test BenchmarkResult with string ID."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id="result_1",
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert result.id == "result_1"


def test_benchmark_result_unallocated_entity_raises_error() -> None:
    """Test that unallocated entity raises ValueError."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    unallocated_pool = source.get_pool()

    with pytest.raises(ValueError, match="is not allocated"):
        BenchmarkResult(
            id=0,
            allocator=allocator,
            source=source,
            entity=unallocated_pool,
            duration=0.5,
        )


def test_benchmark_result_negative_duration_raises_error(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test that negative duration raises ValueError."""
    pool, allocator, source = allocated_pool

    with pytest.raises(ValueError, match="duration must be non-negative"):
        BenchmarkResult(
            id=0,
            allocator=allocator,
            source=source,
            entity=pool,
            duration=-0.5,
        )


def test_benchmark_result_zero_duration(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test BenchmarkResult with zero duration."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.0,
    )
    assert result.duration == 0.0


def test_benchmark_result_allocator_name(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test allocator_name property."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert isinstance(result.allocator_name, str)
    assert len(result.allocator_name) > 0


def test_benchmark_result_allocator_name_from_string(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test allocator_name property when allocator is a string."""
    pool, _, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator="greedy",
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert result.allocator_name == "greedy"


def test_benchmark_result_source_name(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test source_name property."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert isinstance(result.source_name, str)
    assert len(result.source_name) > 0


def test_benchmark_result_source_name_from_string(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test source_name property when source is a string."""
    pool, allocator, _ = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source="random",
        entity=pool,
        duration=0.5,
    )
    assert result.source_name == "random"


def test_benchmark_result_allocation_efficiency(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test allocation_efficiency property."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    efficiency = result.allocation_efficiency
    assert isinstance(efficiency, float)
    assert 0.0 <= efficiency <= 1.0


def test_benchmark_result_num_allocations(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test num_allocations property."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert result.num_allocations == 10


def test_benchmark_result_frozen(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test that BenchmarkResult is frozen (immutable)."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    with pytest.raises(AttributeError):
        result.duration = 1.0  # type: ignore[misc]


def test_benchmark_result_visualize_no_file(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test visualize without saving to file."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    # Should not raise an error
    result.visualize(file_path=None, show_inline=False)


def test_benchmark_result_visualize_with_file(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test visualize with saving to file."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        result.visualize(file_path=tmp_path, show_inline=False)
        assert tmp_path.exists()
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def test_benchmark_result_different_num_allocations() -> None:
    """Test BenchmarkResult with different numbers of allocations."""
    source1 = RandomSource(num_allocations=5, seed=42)
    source2 = RandomSource(num_allocations=15, seed=43)
    allocator = GreedyAllocator()

    pool1 = run_allocation(source1.get_pool(), allocator)
    pool2 = run_allocation(source2.get_pool(), allocator)

    result1 = BenchmarkResult(
        id=0, allocator=allocator, source=source1, entity=pool1, duration=0.5
    )
    result2 = BenchmarkResult(
        id=1, allocator=allocator, source=source2, entity=pool2, duration=0.5
    )

    assert result1.num_allocations == 5
    assert result2.num_allocations == 15


def test_benchmark_result_different_durations(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test BenchmarkResult with different durations."""
    pool, allocator, source = allocated_pool

    result1 = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.1
    )
    result2 = BenchmarkResult(
        id=1, allocator=allocator, source=source, entity=pool, duration=1.5
    )

    assert result1.duration == 0.1
    assert result2.duration == 1.5


def test_benchmark_result_with_allocator_instance(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test BenchmarkResult with allocator instance."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert result.allocator == allocator


def test_benchmark_result_with_source_instance(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test BenchmarkResult with source instance."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert result.source == source


def test_benchmark_result_entity_property(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test entity property returns the allocated pool."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert result.entity == pool
    assert result.entity.is_allocated


def test_benchmark_result_all_allocations_have_offsets(
    allocated_pool: tuple[Any, GreedyAllocator, RandomSource],
) -> None:
    """Test that all allocations in the entity have offsets."""
    pool, allocator, source = allocated_pool
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=pool,
        duration=0.5,
    )
    assert all(alloc.offset is not None for alloc in result.entity.allocations)
