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
from omnimalloc.benchmark import run_benchmark
from omnimalloc.benchmark.results import (
    BenchmarkCampaign,
    BenchmarkReport,
    BenchmarkResult,
)
from omnimalloc.benchmark.results.export import RESULTS_CSV_COLUMNS, save_benchmark
from omnimalloc.benchmark.sources import MinimallocSource, RandomSource
from omnimalloc.benchmark.sources.sync_patterns import SyncPatternSource
from omnimalloc.io import save_allocation
from omnimalloc.primitives import Allocation, Pool

from tests.markers import needs_matplotlib

pytestmark = needs_matplotlib


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


def _rows(output_path: Path) -> list[dict[str, str]]:
    with (output_path / "results.csv").open(newline="") as f:
        return list(csv.DictReader(f))


def test_save_benchmark_creates_directory(
    simple_campaign: BenchmarkCampaign, tmp_path: Path
) -> None:
    result_path = save_benchmark(
        simple_campaign, tmp_path / "out", visualize_iterations=False
    )

    assert result_path == tmp_path / "out"
    assert sorted(p.name for p in result_path.iterdir()) == [
        "campaign_overview.pdf",
        "metadata.json",
        "results.csv",
    ]
    assert json.loads((result_path / "metadata.json").read_text()) == {"test": "value"}


@pytest.mark.parametrize("name", ["out", "out.zip"])
def test_save_benchmark_creates_zip(
    simple_campaign: BenchmarkCampaign, tmp_path: Path, name: str
) -> None:
    result_path = save_benchmark(
        simple_campaign,
        tmp_path / name,
        output_format="zip",
        visualize_iterations=False,
    )

    assert result_path == tmp_path / "out.zip"
    with ZipFile(result_path, "r") as zip_file:
        names = zip_file.namelist()
    assert "out/metadata.json" in names
    assert all(name.startswith("out/") for name in names)


def test_save_benchmark_defaults_to_artifacts_under_cwd(
    simple_campaign: BenchmarkCampaign,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    result_path = save_benchmark(simple_campaign, visualize_iterations=False)
    assert result_path == tmp_path / "artifacts" / "test_campaign"


def test_save_benchmark_rejects_invalid_arguments(
    simple_campaign: BenchmarkCampaign, tmp_path: Path
) -> None:
    with pytest.raises(TypeError, match="Expected a BenchmarkCampaign"):
        save_benchmark(simple_campaign.reports[0], tmp_path)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="output_format must be 'dir' or 'zip'"):
        save_benchmark(simple_campaign, tmp_path, output_format="tar")  # type: ignore[arg-type]


def test_save_benchmark_overwrites_only_when_allowed(
    simple_campaign: BenchmarkCampaign, tmp_path: Path
) -> None:
    output_path = tmp_path / "out"
    save_benchmark(simple_campaign, output_path)
    (output_path / "stale").touch()

    save_benchmark(simple_campaign, output_path)
    assert not (output_path / "stale").exists()
    with pytest.raises(FileExistsError, match="already exists"):
        save_benchmark(simple_campaign, output_path, overwrite=False)


def test_save_benchmark_writes_results_csv(
    simple_campaign: BenchmarkCampaign, tmp_path: Path
) -> None:
    rows = _rows(
        save_benchmark(simple_campaign, tmp_path / "out", visualize_iterations=False)
    )

    assert tuple(rows[0]) == RESULTS_CSV_COLUMNS
    assert rows == [
        rows[0]
        | {
            "source": "random",
            "allocator": "greedy",
            "variant": "10",
            "num_allocations": "10",
            "iterations": "1",
            "mean_seconds": "0.5",
            "stdev_seconds": "",
            "known_optimum": "",
            "optimum_ratio": "",
        }
    ]


def test_results_csv_has_one_row_per_report(tmp_path: Path) -> None:
    campaign = run_benchmark(
        allocators=("greedy",),
        sources=(RandomSource(num_allocations=10, seed=42),),
        variants=(10, 20, 30),
        iterations=2,
    )
    rows = _rows(save_benchmark(campaign, tmp_path / "out", visualize_iterations=False))

    assert [row["variant"] for row in rows] == ["10", "20", "30"]
    assert all(float(row["stdev_seconds"]) >= 0 for row in rows)


def test_results_csv_leaves_unmeasurable_efficiency_empty(tmp_path: Path) -> None:
    source = SyncPatternSource(
        num_allocations=2000, num_threads=64, pattern="independent"
    )
    campaign = run_benchmark(allocators=("greedy",), sources=(source,))
    rows = _rows(save_benchmark(campaign, tmp_path / "out", visualize_iterations=False))

    assert rows[0]["mean_efficiency"] == ""
    assert rows[0]["lower_bound"] == ""
    assert int(rows[0]["mean_peak_size"]) > 0


def test_save_benchmark_plots_iterations_one_dir_per_label(tmp_path: Path) -> None:
    csv_dir = tmp_path / "csvs"
    csv_dir.mkdir()
    pool = Pool(id="p", allocations=(Allocation(id=0, size=8, start=0, end=4),))
    save_allocation(pool, csv_dir / "p.csv")
    campaign = run_benchmark(
        allocators=("greedy",), sources=(MinimallocSource(csv_dir=csv_dir),)
    )

    output_path = save_benchmark(campaign, tmp_path / "out")

    (source_dir,) = (output_path / "sources").iterdir()
    iteration = source_dir / "allocators" / "greedy" / "p" / "iterations"
    assert (iteration / "iteration_0.pdf").is_file()
