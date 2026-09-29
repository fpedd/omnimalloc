#
# SPDX-License-Identifier: Apache-2.0
#

import csv
import json
import logging
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Final, Literal

from ..utils import tqdm  # noqa: TID252
from .campaign import BenchmarkCampaign
from .report import BenchmarkReport
from .visualize import plot_benchmark

logger = logging.getLogger(__name__)

# Stable, self-explanatory schema: cross-run regression tracking joins on it
RESULTS_CSV_COLUMNS: Final[tuple[str, ...]] = (
    "source",
    "allocator",
    "variant",
    "num_allocations",
    "iterations",
    "mean_seconds",
    "median_seconds",
    "stdev_seconds",
    "min_seconds",
    "max_seconds",
    "mean_peak_size",
    "lower_bound",
    "mean_efficiency",
    "known_optimum",
    "optimum_ratio",
)


def _prepare_base_dir(output_path: Path, output_format: str) -> Path:
    if output_format == "dir":
        output_path.mkdir(parents=True)
        return output_path
    base_dir = Path(tempfile.mkdtemp(prefix="omnimalloc_dump_")) / output_path.stem
    base_dir.mkdir()
    return base_dir


def _write_metadata(base_dir: Path, campaign: BenchmarkCampaign) -> None:
    metadata_file = base_dir / "metadata.json"
    with metadata_file.open("w") as f:
        json.dump(campaign.metadata, f, indent=2, default=str)


def _report_row(report: BenchmarkReport) -> dict[str, Any]:
    return {
        "source": report.source_name,
        "allocator": report.allocator_name,
        "variant": report.variant_label,
        "num_allocations": report.num_allocations,
        "iterations": report.num_results,
        "mean_seconds": report.mean_seconds,
        "median_seconds": report.median_seconds,
        "stdev_seconds": report.stdev_seconds,
        "min_seconds": report.min_seconds,
        "max_seconds": report.max_seconds,
        "mean_peak_size": report.mean_peak_size,
        "lower_bound": report.lower_bound,
        "mean_efficiency": report.mean_allocation_efficiency,
        "known_optimum": report.known_optimum,
        "optimum_ratio": report.optimum_ratio,
    }


def _write_results_csv(base_dir: Path, campaign: BenchmarkCampaign) -> None:
    """Write one flat row per report, the machine-readable campaign summary."""
    with (base_dir / "results.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS_CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(_report_row(report) for report in campaign.reports)


def _create_zip_archive(base_dir: Path, final_path: Path) -> Path:
    return Path(
        shutil.make_archive(
            str(final_path.with_suffix("")),
            "zip",
            root_dir=base_dir.parent,
            base_dir=base_dir.name,
        )
    )


def _dir_name(label: str) -> str:
    """A label as one path component; labels may carry an explicit path."""
    return re.sub(r"[/\\]", "_", label)


def _write_iterations(base_dir: Path, campaign: BenchmarkCampaign) -> None:
    """Plot every iteration under sources/<source>/allocators/<allocator>/<variant>."""
    for report in tqdm(campaign.reports, desc="Saving iterations", leave=False):
        iterations_dir = (
            base_dir
            / "sources"
            / _dir_name(report.source_name)
            / "allocators"
            / _dir_name(report.allocator_name)
            / _dir_name(report.variant_label)
            / "iterations"
        )
        for i, result in enumerate(report.results):
            result.visualize(iterations_dir / f"iteration_{i}.pdf")


def save_benchmark(
    campaign: BenchmarkCampaign,
    output_path: Path | str | None = None,
    output_format: Literal["dir", "zip"] = "dir",
    visualize_iterations: bool = True,
    overwrite: bool = True,
) -> Path:
    """Save a campaign, defaulting to `artifacts/<id>` under the cwd."""

    if not isinstance(campaign, BenchmarkCampaign):
        raise TypeError(f"Expected a BenchmarkCampaign, got {type(campaign)!r}")

    if output_path is None:
        output_path = Path.cwd() / "artifacts" / str(campaign.id)

    output_path = Path(output_path)

    if output_format not in ("dir", "zip"):
        raise ValueError(f"output_format must be 'dir' or 'zip', got {output_format!r}")

    final_path = (
        output_path if output_format == "dir" else output_path.with_suffix(".zip")
    )

    if final_path.exists():
        if not overwrite:
            raise FileExistsError(f"Output {final_path} already exists.")
        if final_path.is_dir():
            shutil.rmtree(final_path)
        else:
            final_path.unlink()

    base_dir = _prepare_base_dir(output_path, output_format)

    try:
        _write_metadata(base_dir, campaign)
        _write_results_csv(base_dir, campaign)
        plot_benchmark(campaign, base_dir / "campaign_overview.pdf")
        if visualize_iterations:
            _write_iterations(base_dir, campaign)

        if output_format == "zip":
            final_path = _create_zip_archive(base_dir, final_path)
            logger.info(f"Campaign dumped to zip: {final_path}")
        else:
            logger.info(f"Campaign dumped to directory: {final_path}")

        return final_path

    finally:
        if output_format == "zip":
            shutil.rmtree(base_dir.parent, ignore_errors=True)
