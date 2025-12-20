#
# SPDX-License-Identifier: Apache-2.0
#

import tempfile
from pathlib import Path

import pytest
from omnimalloc.benchmark.sources.minimalloc import MinimallocSource
from omnimalloc.primitives import BufferKind


@pytest.fixture
def sample_csv_path() -> Path:
    """Create a temporary CSV file with sample minimalloc data."""
    content = """id,lower,upper,size
0,0,3,4
1,3,9,4
2,0,9,8
3,9,21,4
4,0,21,16
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return Path(f.name)


def test_minimalloc_source_basic_creation(sample_csv_path: Path) -> None:
    source = MinimallocSource(sample_csv_path)
    assert source.file_path == sample_csv_path
    assert source.num_allocations == 5


def test_minimalloc_source_get_allocations(sample_csv_path: Path) -> None:
    source = MinimallocSource(sample_csv_path)
    allocations = source.get_allocations()
    assert len(allocations) == 5
    assert allocations[0].id == "0"
    assert allocations[0].start == 0
    assert allocations[0].end == 3
    assert allocations[0].size == 4
    assert allocations[0].kind == BufferKind.WORKSPACE


def test_minimalloc_source_get_allocations_with_count(sample_csv_path: Path) -> None:
    source = MinimallocSource(sample_csv_path)
    allocations = source.get_allocations(num_allocations=3)
    assert len(allocations) == 3
    assert allocations[0].id == "0"
    assert allocations[2].id == "2"


def test_minimalloc_source_get_allocations_with_skip(sample_csv_path: Path) -> None:
    source = MinimallocSource(sample_csv_path)
    allocations = source.get_allocations(skip=2)
    assert len(allocations) == 3
    assert allocations[0].id == "2"
    assert allocations[2].id == "4"


def test_minimalloc_source_get_allocations_skip_and_count(
    sample_csv_path: Path,
) -> None:
    source = MinimallocSource(sample_csv_path)
    allocations = source.get_allocations(num_allocations=2, skip=1)
    assert len(allocations) == 2
    assert allocations[0].id == "1"
    assert allocations[1].id == "2"


def test_minimalloc_source_get_allocations_skip_past_end(sample_csv_path: Path) -> None:
    source = MinimallocSource(sample_csv_path)
    allocations = source.get_allocations(skip=10)
    assert len(allocations) == 0


def test_minimalloc_source_get_pools(sample_csv_path: Path) -> None:
    source = MinimallocSource(sample_csv_path)
    pools = source.get_pools()
    assert len(pools) == 1
    assert len(pools[0].allocations) == 5
    assert pools[0].id == sample_csv_path.stem


def test_minimalloc_source_get_pools_with_skip(sample_csv_path: Path) -> None:
    source = MinimallocSource(sample_csv_path)
    pools = source.get_pools(skip=1)
    assert len(pools) == 0


def test_minimalloc_source_get_pools_count_zero(sample_csv_path: Path) -> None:
    source = MinimallocSource(sample_csv_path)
    pools = source.get_pools(num_pools=0)
    assert len(pools) == 0


def test_minimalloc_source_get_pool(sample_csv_path: Path) -> None:
    source = MinimallocSource(sample_csv_path)
    pool = source.get_pool()
    assert len(pool.allocations) == 5


def test_minimalloc_source_get_allocation(sample_csv_path: Path) -> None:
    source = MinimallocSource(sample_csv_path)
    allocation = source.get_allocation()
    assert allocation.id == "0"
    assert allocation.size == 4


def test_minimalloc_source_file_not_found() -> None:
    """Test that appropriate error is raised for missing file."""
    with pytest.raises(FileNotFoundError):
        MinimallocSource("/nonexistent/path.csv")


def test_minimalloc_source_str_path(sample_csv_path: Path) -> None:
    """Test that string paths are accepted."""
    source = MinimallocSource(str(sample_csv_path))
    assert source.file_path == sample_csv_path
    allocations = source.get_allocations()
    assert len(allocations) == 5
