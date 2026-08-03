#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.benchmark.sources.base import BaseSource
from omnimalloc.benchmark.sources.generator import (
    HighContentionSource,
    PowerOf2Source,
    RandomSource,
    SequentialSource,
    UniformSource,
)
from omnimalloc.primitives import AllocationKind

SOURCES = [
    RandomSource,
    UniformSource,
    PowerOf2Source,
    HighContentionSource,
    SequentialSource,
]


@pytest.mark.parametrize("source_cls", SOURCES)
def test_source_basic_creation(source_cls: type[BaseSource]) -> None:
    source = source_cls(num_allocations=10, seed=42)
    allocations = source.get_allocations()
    assert len(allocations) == 10
    assert all(alloc.id == i for i, alloc in enumerate(allocations))


@pytest.mark.parametrize("source_cls", SOURCES)
def test_source_count_parameter(source_cls: type[BaseSource]) -> None:
    source = source_cls(num_allocations=100, seed=42)
    allocations = source.get_allocations(num_allocations=5)
    assert len(allocations) == 5


@pytest.mark.parametrize("source_cls", SOURCES)
def test_source_skip_parameter(source_cls: type[BaseSource]) -> None:
    source = source_cls(num_allocations=10, seed=42)
    allocs_no_skip = source.get_allocations()
    allocs_skip = source.get_allocations(skip=3)
    assert allocs_skip[0].id == 3
    assert allocs_no_skip[3].size == allocs_skip[0].size
    assert allocs_no_skip[3].start == allocs_skip[0].start


@pytest.mark.parametrize("source_cls", SOURCES)
def test_source_determinism(source_cls: type[BaseSource]) -> None:
    allocs1 = source_cls(num_allocations=10, seed=42).get_allocations()
    allocs2 = source_cls(num_allocations=10, seed=42).get_allocations()
    assert [(a.size, a.start, a.end) for a in allocs1] == [
        (a.size, a.start, a.end) for a in allocs2
    ]


def test_random_source_size_bounds() -> None:
    source = RandomSource(num_allocations=100, size_min=1024, size_max=2048, seed=42)
    allocations = source.get_allocations()
    assert all(1024 <= alloc.size <= 2048 for alloc in allocations)


def test_random_source_time_bounds() -> None:
    source = RandomSource(
        num_allocations=100, time_min=10, time_max=100, seed=42, duration_max=5
    )
    allocations = source.get_allocations()
    assert all(alloc.start >= 10 for alloc in allocations)
    assert all(alloc.end <= 100 for alloc in allocations)


def test_random_source_duration_bounds() -> None:
    source = RandomSource(
        num_allocations=100, duration_min=5, duration_max=10, time_max=1000, seed=42
    )
    allocations = source.get_allocations()
    assert all(5 <= alloc.duration <= 10 for alloc in allocations)


def test_random_source_allocation_kinds() -> None:
    kinds = (AllocationKind.WORKSPACE, AllocationKind.CONSTANT)
    source = RandomSource(num_allocations=50, kinds=kinds, seed=42)
    allocations = source.get_allocations()
    assert all(alloc.kind in kinds for alloc in allocations)


def test_random_source_allocation_kinds_with_weights() -> None:
    kinds = (AllocationKind.WORKSPACE, AllocationKind.CONSTANT)
    weights = (0.8, 0.2)
    source = RandomSource(
        num_allocations=100, kinds=kinds, kind_weights=weights, seed=42
    )
    allocations = source.get_allocations()
    workspace_count = sum(1 for a in allocations if a.kind == AllocationKind.WORKSPACE)
    assert workspace_count > 60


def test_random_source_validation_count() -> None:
    with pytest.raises(ValueError, match="num_allocations must be positive"):
        RandomSource(num_allocations=0)


def test_random_source_validation_size_min() -> None:
    with pytest.raises(ValueError, match="size_min must be positive"):
        RandomSource(size_min=0)


def test_random_source_validation_size_max() -> None:
    with pytest.raises(ValueError, match="size_max must be >= size_min"):
        RandomSource(size_min=100, size_max=50)


def test_random_source_validation_time_min() -> None:
    with pytest.raises(ValueError, match="time_min must be non-negative"):
        RandomSource(time_min=-1)


def test_random_source_validation_time_max() -> None:
    with pytest.raises(ValueError, match="time_max must be > time_min"):
        RandomSource(time_min=100, time_max=100)


def test_random_source_validation_duration_min() -> None:
    with pytest.raises(ValueError, match="duration_min must be positive"):
        RandomSource(duration_min=0)


def test_random_source_validation_duration_max() -> None:
    with pytest.raises(ValueError, match="duration_max must be >= duration_min"):
        RandomSource(duration_min=10, duration_max=5)


def test_random_source_validation_kind_weights() -> None:
    with pytest.raises(
        ValueError, match="kinds and kind_weights must have same length"
    ):
        RandomSource(kinds=(AllocationKind.WORKSPACE,), kind_weights=(0.5, 0.5))


def test_uniform_source_fixed_size_and_duration() -> None:
    source = UniformSource(num_allocations=10, size=4096, duration=5, seed=42)
    allocations = source.get_allocations()
    assert all(alloc.size == 4096 for alloc in allocations)
    assert all(alloc.duration == 5 for alloc in allocations)


def test_uniform_source_random_start_times() -> None:
    source = UniformSource(num_allocations=100, time_max=50, duration=5, seed=42)
    allocations = source.get_allocations()
    start_times = {alloc.start for alloc in allocations}
    assert len(start_times) > 10


def test_power_of_2_source_sizes() -> None:
    source = PowerOf2Source(
        num_allocations=100, size_exponent_min=10, size_exponent_max=15, seed=42
    )
    allocations = source.get_allocations()
    for alloc in allocations:
        assert alloc.size & (alloc.size - 1) == 0
        assert 2**10 <= alloc.size <= 2**15


def test_power_of_2_source_duration_bounds() -> None:
    source = PowerOf2Source(
        num_allocations=100, duration_min=3, duration_max=7, seed=42
    )
    allocations = source.get_allocations()
    assert all(3 <= alloc.duration <= 7 for alloc in allocations)


def test_high_contention_source_time_window() -> None:
    source = HighContentionSource(num_allocations=100, time_window=50, seed=42)
    allocations = source.get_allocations()
    assert all(alloc.start >= 0 for alloc in allocations)
    assert all(alloc.end <= 50 for alloc in allocations)


def test_high_contention_source_high_contention() -> None:
    source = HighContentionSource(num_allocations=100, time_window=20, seed=42)
    allocations = source.get_allocations()
    overlaps = 0
    for i, a1 in enumerate(allocations):
        for a2 in allocations[i + 1 :]:
            if a1.conflicts_with(a2):
                overlaps += 1
    assert overlaps > 1000


def test_sequential_source_minimal_overlap() -> None:
    source = SequentialSource(num_allocations=100, seed=42)
    allocations = source.get_allocations()
    overlaps = 0
    for i, a1 in enumerate(allocations):
        for a2 in allocations[i + 1 :]:
            if a1.conflicts_with(a2):
                overlaps += 1
    assert overlaps < 200


def test_sequential_source_sequential_ordering() -> None:
    source = SequentialSource(num_allocations=50, seed=42)
    allocations = source.get_allocations()
    for i in range(len(allocations) - 1):
        assert allocations[i + 1].start >= allocations[i].start - 2


def test_sequential_source_skip_beyond_configured_count() -> None:
    source = SequentialSource(num_allocations=5, seed=42)
    allocs_skip_5 = source.get_allocations(skip=5)
    assert len(allocs_skip_5) == 5
    assert allocs_skip_5[0].id == 5
    assert allocs_skip_5[4].id == 9


def test_sequential_source_size_bounds() -> None:
    source = SequentialSource(num_allocations=100, size_min=512, size_max=1024, seed=42)
    allocations = source.get_allocations()
    assert all(512 <= alloc.size <= 1024 for alloc in allocations)


def test_sequential_source_duration_bounds() -> None:
    source = SequentialSource(
        num_allocations=100, duration_min=8, duration_max=12, seed=42
    )
    allocations = source.get_allocations()
    assert all(8 <= alloc.duration <= 12 for alloc in allocations)
