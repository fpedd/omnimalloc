#
# SPDX-License-Identifier: Apache-2.0
#


import tempfile
from pathlib import Path
from typing import Any

import pytest
from omnimalloc import allocate
from omnimalloc.allocators import GreedyAllocator
from omnimalloc.benchmark.results.result import BenchmarkResult
from omnimalloc.benchmark.sources.generator import RandomSource

from tests.markers import needs_matplotlib

Fixture = tuple[Any, GreedyAllocator, RandomSource]


@pytest.fixture  # type: ignore[misc]
def allocated_pool() -> Fixture:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)
    return pool, allocator, source


def _make(allocated_pool: Fixture, **overrides: object) -> BenchmarkResult:
    pool, allocator, source = allocated_pool
    kwargs: dict[str, Any] = {
        "id": 0,
        "allocator": allocator,
        "source": source,
        "entity": pool,
        "duration": 0.5,
    }
    kwargs.update(overrides)
    return BenchmarkResult(**kwargs)


def test_benchmark_result_creation_basic(allocated_pool: Fixture) -> None:
    pool, allocator, source = allocated_pool
    result = _make(allocated_pool)
    assert result.id == 0
    assert result.duration == 0.5
    assert result.entity == pool
    assert result.entity.is_allocated
    assert result.allocator == allocator
    assert result.source == source


def test_benchmark_result_id_string(allocated_pool: Fixture) -> None:
    assert _make(allocated_pool, id="result_1").id == "result_1"


def test_benchmark_result_unallocated_entity_raises_error() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    with pytest.raises(ValueError, match="is not allocated"):
        BenchmarkResult(
            id=0,
            allocator=GreedyAllocator(),
            source=source,
            entity=source.get_pool(),
            duration=0.5,
        )


def test_benchmark_result_negative_duration_raises_error(
    allocated_pool: Fixture,
) -> None:
    with pytest.raises(ValueError, match="duration must be non-negative"):
        _make(allocated_pool, duration=-0.5)


def test_benchmark_result_zero_duration(allocated_pool: Fixture) -> None:
    assert _make(allocated_pool, duration=0.0).duration == 0.0


def test_benchmark_result_properties(allocated_pool: Fixture) -> None:
    result = _make(allocated_pool)
    assert isinstance(result.allocator_name, str)
    assert result.allocator_name
    assert isinstance(result.source_name, str)
    assert result.source_name
    assert 0.0 <= result.allocation_efficiency <= 1.0
    assert result.num_allocations == 10


def test_benchmark_result_allocator_name_from_string(
    allocated_pool: Fixture,
) -> None:
    assert _make(allocated_pool, allocator="greedy").allocator_name == "greedy"


def test_benchmark_result_source_name_from_string(allocated_pool: Fixture) -> None:
    assert _make(allocated_pool, source="random").source_name == "random"


def test_benchmark_result_frozen(allocated_pool: Fixture) -> None:
    result = _make(allocated_pool)
    with pytest.raises(AttributeError):
        result.duration = 1.0  # type: ignore[misc]


@pytest.mark.filterwarnings("ignore::UserWarning")
@needs_matplotlib
def test_benchmark_result_visualize_no_file(allocated_pool: Fixture) -> None:
    _make(allocated_pool).visualize()


@needs_matplotlib
def test_benchmark_result_visualize_with_file(allocated_pool: Fixture) -> None:
    result = _make(allocated_pool)
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        result.visualize(tmp_path)
        assert tmp_path.exists()
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def test_benchmark_result_different_num_allocations() -> None:
    source1 = RandomSource(num_allocations=5, seed=42)
    source2 = RandomSource(num_allocations=15, seed=43)
    allocator = GreedyAllocator()

    pool1 = allocate(source1.get_pool(), allocator)
    pool2 = allocate(source2.get_pool(), allocator)

    result1 = BenchmarkResult(
        id=0, allocator=allocator, source=source1, entity=pool1, duration=0.5
    )
    result2 = BenchmarkResult(
        id=1, allocator=allocator, source=source2, entity=pool2, duration=0.5
    )

    assert result1.num_allocations == 5
    assert result2.num_allocations == 15
