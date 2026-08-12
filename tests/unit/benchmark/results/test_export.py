#
# SPDX-License-Identifier: Apache-2.0
#


import csv
import json
from pathlib import Path
from zipfile import ZipFile

import pytest
from omnimalloc import allocate
from omnimalloc.allocators import GreedyAllocator
from omnimalloc.benchmark.results import (
    BenchmarkCampaign,
    BenchmarkReport,
    BenchmarkResult,
)
from omnimalloc.benchmark.results.export import (
    RESULTS_CSV_COLUMNS,
    _prepare_base_dir,
    _write_metadata,
    save_benchmark,
)
from omnimalloc.benchmark.sources.generator import RandomSource
from omnimalloc.benchmark.sources.sync_patterns import SyncPatternSource

from tests.markers import needs_matplotlib


@pytest.fixture
def simple_campaign() -> BenchmarkCampaign:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)

    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    report = BenchmarkReport(id=0, results=(result,))
    return BenchmarkCampaign(
        id="test_campaign", reports=(report,), metadata={"test": "value"}
    )


@needs_matplotlib
def test_save_benchmark_creates_directory(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
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


@needs_matplotlib
def test_save_benchmark_creates_zip(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
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

    with ZipFile(result_path, "r") as zip_file:
        names = zip_file.namelist()
        assert any("metadata.json" in name for name in names)
        assert any("campaign_overview.pdf" in name for name in names)


@needs_matplotlib
def test_save_benchmark_with_none_path(
    simple_campaign: BenchmarkCampaign,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result_path = save_benchmark(
        simple_campaign,
        output_path=None,
        output_format="dir",
        visualize_iterations=False,
    )

    assert result_path.exists()
    assert result_path.is_relative_to(tmp_path)
    assert "campaign_test_campaign" in str(result_path)


def test_save_benchmark_raises_typeerror_for_non_campaign(artifacts_dir: Path) -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)
    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )

    with pytest.raises(TypeError, match="Expected a BenchmarkCampaign"):
        save_benchmark(result, output_path=artifacts_dir / "output")  # type: ignore[arg-type]


def test_save_benchmark_raises_valueerror_for_invalid_format(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    with pytest.raises(ValueError, match="output_format must be 'dir' or 'zip'"):
        save_benchmark(
            simple_campaign,
            output_path=artifacts_dir / "output",
            output_format="invalid",  # type: ignore[arg-type]
        )


@needs_matplotlib
def test_save_benchmark_raises_fileexistserror_when_not_overwriting(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    output_path = artifacts_dir / "campaign_output"

    save_benchmark(
        simple_campaign, output_path=output_path, output_format="dir", overwrite=True
    )

    with pytest.raises(FileExistsError, match="already exists"):
        save_benchmark(
            simple_campaign,
            output_path=output_path,
            output_format="dir",
            overwrite=False,
        )


@needs_matplotlib
def test_save_benchmark_overwrites_existing_directory(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    output_path = artifacts_dir / "campaign_output"

    result1 = save_benchmark(
        simple_campaign, output_path=output_path, output_format="dir", overwrite=True
    )
    result2 = save_benchmark(
        simple_campaign, output_path=output_path, output_format="dir", overwrite=True
    )

    assert result1 == result2
    assert result2.exists()


def test_write_metadata_creates_json_file(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    _write_metadata(artifacts_dir, simple_campaign)

    metadata_file = artifacts_dir / "metadata.json"
    assert metadata_file.exists()

    with metadata_file.open("r") as f:
        metadata = json.load(f)
        assert "test" in metadata
        assert metadata["test"] == "value"


def test_prepare_base_dir_creates_directory(artifacts_dir: Path) -> None:
    output_path = artifacts_dir / "test_dir"

    base_dir = _prepare_base_dir(output_path, output_format="dir", overwrite=True)

    assert base_dir == output_path
    assert base_dir.exists()
    assert base_dir.is_dir()


def test_prepare_base_dir_creates_temp_for_zip(artifacts_dir: Path) -> None:
    output_path = artifacts_dir / "test_zip"

    base_dir = _prepare_base_dir(output_path, output_format="zip", overwrite=True)

    assert base_dir != output_path
    assert base_dir.exists()
    assert "omnimalloc_dump_" in str(base_dir)


@needs_matplotlib
def test_save_benchmark_writes_results_csv(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    output_path = save_benchmark(
        simple_campaign,
        output_path=artifacts_dir / "campaign_output",
        output_format="dir",
        visualize_iterations=False,
    )

    with (output_path / "results.csv").open(newline="") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == simple_campaign.num_reports
    assert tuple(rows[0]) == RESULTS_CSV_COLUMNS
    assert rows[0]["source"] == "random"
    assert rows[0]["allocator"] == "greedy"
    assert rows[0]["num_allocations"] == "10"
    assert rows[0]["iterations"] == "1"
    assert float(rows[0]["mean_seconds"]) == 0.5
    assert rows[0]["stdev_seconds"] == ""
    assert rows[0]["known_optimum"] == ""
    assert rows[0]["optimum_ratio"] == ""


@needs_matplotlib
def test_results_csv_has_one_row_per_report(artifacts_dir: Path) -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)
    reports = tuple(
        BenchmarkReport(
            id=i,
            results=tuple(
                BenchmarkResult(
                    id=10 * i + j,
                    allocator=allocator,
                    source=source,
                    entity=pool,
                    duration=float(j + 1),
                )
                for j in range(2)
            ),
            variant_id=10 * (i + 1),
            known_optimum=pool.pressure,
        )
        for i in range(3)
    )
    campaign = BenchmarkCampaign(id="multi", reports=reports)

    output_path = save_benchmark(
        campaign,
        output_path=artifacts_dir / "multi_output",
        output_format="dir",
        visualize_iterations=False,
    )

    with (output_path / "results.csv").open(newline="") as f:
        rows = list(csv.DictReader(f))

    assert [row["variant"] for row in rows] == ["10", "20", "30"]
    assert all(float(row["stdev_seconds"]) > 0 for row in rows)
    assert all(float(row["optimum_ratio"]) >= 1.0 for row in rows)
    assert all(int(row["lower_bound"]) == pool.pressure for row in rows)


@needs_matplotlib
def test_results_csv_leaves_unmeasurable_efficiency_empty(artifacts_dir: Path) -> None:
    source = SyncPatternSource(
        num_allocations=2000, num_threads=64, pattern="independent"
    )
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)
    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    campaign = BenchmarkCampaign(
        id="wide", reports=(BenchmarkReport(id=0, results=(result,), source=source),)
    )

    output_path = save_benchmark(
        campaign,
        output_path=artifacts_dir / "wide_output",
        output_format="dir",
        visualize_iterations=False,
    )

    with (output_path / "results.csv").open(newline="") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    assert rows[0]["mean_efficiency"] == ""
    assert rows[0]["lower_bound"] == ""
    assert float(rows[0]["mean_seconds"]) == 0.5
    assert int(rows[0]["mean_peak_size"]) > 0
    assert (output_path / "campaign_overview.pdf").exists()


@needs_matplotlib
def test_save_benchmark_zip_path_lands_exactly_there(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    output_path = artifacts_dir / "campaign_output.zip"
    result_path = save_benchmark(
        simple_campaign,
        output_path=output_path,
        output_format="zip",
        visualize_iterations=False,
    )
    assert result_path == output_path
    assert result_path.is_file()
    assert not output_path.with_suffix(".zip.zip").exists()


def test_zip_internal_folder_uses_campaign_name(
    simple_campaign: BenchmarkCampaign, artifacts_dir: Path
) -> None:
    output_path = artifacts_dir / "named_campaign"
    result_path = save_benchmark(
        simple_campaign,
        output_path=output_path,
        output_format="zip",
        visualize_iterations=False,
    )
    with ZipFile(result_path, "r") as zip_file:
        assert all(name.startswith("named_campaign/") for name in zip_file.namelist())
