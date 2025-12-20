#
# SPDX-License-Identifier: Apache-2.0
#


import json
from pathlib import Path
from zipfile import ZipFile

import pytest
from omnimalloc import run_allocation
from omnimalloc.allocators import GreedyAllocator
from omnimalloc.benchmark.results import (
    BenchmarkCampaign,
    BenchmarkReport,
    BenchmarkResult,
)
from omnimalloc.benchmark.results.export import (
    _prepare_base_dir,
    _write_metadata,
    save_benchmark,
)
from omnimalloc.benchmark.sources.generator import RandomSource


@pytest.fixture
def simple_campaign() -> BenchmarkCampaign:
    """Create a simple benchmark campaign for testing."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = run_allocation(source.get_pool(), allocator)

    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    report = BenchmarkReport(id=0, results=(result,))
    return BenchmarkCampaign(
        id="test_campaign", reports=(report,), metadata={"test": "value"}
    )


def test_save_benchmark_creates_directory(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    """Test that save_benchmark creates a directory with expected structure."""
    output_path = artifacts_dir / "campaign_output"

    result_path = save_benchmark(
        simple_campaign,
        output_path=output_path,
        output_format="dir",
        visualize_iterations=False,
    )

    assert result_path.exists()
    assert result_path.is_dir()
    assert (result_path / "metadata.json").exists()
    assert (result_path / "campaign_overview.pdf").exists()


def test_save_benchmark_creates_zip(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    """Test that save_benchmark creates a zip archive."""
    output_path = artifacts_dir / "campaign_output"

    result_path = save_benchmark(
        simple_campaign,
        output_path=output_path,
        output_format="zip",
        visualize_iterations=False,
    )

    assert result_path.exists()
    assert result_path.suffix == ".zip"
    assert result_path.is_file()

    # Verify zip contents
    with ZipFile(result_path, "r") as zip_file:
        names = zip_file.namelist()
        assert any("metadata.json" in name for name in names)
        assert any("campaign_overview.pdf" in name for name in names)


def test_save_benchmark_with_none_path(simple_campaign: BenchmarkCampaign) -> None:
    """Test that save_benchmark works with None as output_path."""
    result_path = save_benchmark(
        simple_campaign,
        output_path=None,
        output_format="dir",
        visualize_iterations=False,
    )

    assert result_path.exists()
    assert "campaign_test_campaign" in str(result_path)


def test_save_benchmark_raises_typeerror_for_non_campaign(artifacts_dir: Path) -> None:
    """Test that save_benchmark raises TypeError for non-campaign objects."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = run_allocation(source.get_pool(), allocator)
    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )

    with pytest.raises(TypeError, match="only supports BenchmarkCampaign"):
        save_benchmark(result, output_path=artifacts_dir / "output")


def test_save_benchmark_raises_valueerror_for_invalid_format(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    """Test that save_benchmark raises ValueError for invalid output format."""
    with pytest.raises(ValueError, match="output_format must be 'dir' or 'zip'"):
        save_benchmark(
            simple_campaign,
            output_path=artifacts_dir / "output",
            output_format="invalid",  # type: ignore[arg-type]
        )


def test_save_benchmark_raises_fileexistserror_when_not_overwriting(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    """Test that save_benchmark raises FileExistsError when overwrite=False."""
    output_path = artifacts_dir / "campaign_output"

    # First save
    save_benchmark(
        simple_campaign, output_path=output_path, output_format="dir", overwrite=True
    )

    # Second save should fail with overwrite=False
    with pytest.raises(FileExistsError, match="already exists"):
        save_benchmark(
            simple_campaign,
            output_path=output_path,
            output_format="dir",
            overwrite=False,
        )


def test_save_benchmark_overwrites_existing_directory(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    """Test that save_benchmark can overwrite existing directory."""
    output_path = artifacts_dir / "campaign_output"

    # First save
    result1 = save_benchmark(
        simple_campaign, output_path=output_path, output_format="dir", overwrite=True
    )

    # Second save with overwrite=True should succeed
    result2 = save_benchmark(
        simple_campaign, output_path=output_path, output_format="dir", overwrite=True
    )

    assert result1 == result2
    assert result2.exists()


def test_write_metadata_creates_json_file(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    """Test that _write_metadata creates a valid JSON file."""
    _write_metadata(artifacts_dir, simple_campaign)

    metadata_file = artifacts_dir / "metadata.json"
    assert metadata_file.exists()

    with metadata_file.open("r") as f:
        metadata = json.load(f)
        assert "test" in metadata
        assert metadata["test"] == "value"


def test_prepare_base_dir_creates_directory(artifacts_dir: Path) -> None:
    """Test that _prepare_base_dir creates a directory for dir format."""
    output_path = artifacts_dir / "test_dir"

    base_dir = _prepare_base_dir(output_path, output_format="dir", overwrite=True)

    assert base_dir == output_path
    assert base_dir.exists()
    assert base_dir.is_dir()


def test_prepare_base_dir_creates_temp_for_zip(artifacts_dir: Path) -> None:
    """Test that _prepare_base_dir creates a temp directory for zip format."""
    output_path = artifacts_dir / "test_zip"

    base_dir = _prepare_base_dir(output_path, output_format="zip", overwrite=True)

    assert base_dir != output_path
    assert base_dir.exists()
    assert "omnimalloc_dump_" in str(base_dir)
