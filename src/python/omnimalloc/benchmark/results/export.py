#
# SPDX-License-Identifier: Apache-2.0
#

import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Literal, Protocol

from omnimalloc.common.directories import PROJECT_DIR

from .campaign import BenchmarkCampaign
from .report import BenchmarkReport
from .result import BenchmarkResult
from .visualize import plot_benchmark


class ProgressBar(Protocol):
    """Protocol for progress bar objects."""

    def update(self, n: int = 1) -> None:
        """Update the progress bar."""
        ...


try:
    from tqdm.auto import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

    # Fallback: tqdm without progress bars (just returns iterable)
    def tqdm(iterable: Any = None, **kwargs: Any) -> Any:  # noqa: ARG001, ANN401
        """No-op tqdm fallback when tqdm is not installed."""
        if iterable is None:
            # When called with total= instead of an iterable
            class DummyProgressBar:
                def __enter__(self) -> "DummyProgressBar":
                    return self

                def __exit__(self, *args: object) -> None:
                    pass

                def update(self, n: int = 1) -> None:
                    pass

            return DummyProgressBar()
        return iterable


logger = logging.getLogger(__name__)


def _prepare_base_dir(output_path: Path, output_format: str, overwrite: bool) -> Path:
    if output_format == "dir":
        output_path.mkdir(parents=True, exist_ok=overwrite)
        return output_path
    temp_dir = tempfile.mkdtemp(prefix="omnimalloc_dump_")
    return Path(temp_dir)


def _write_metadata(base_dir: Path, campaign: BenchmarkCampaign) -> None:
    metadata_file = base_dir / "metadata.json"
    with metadata_file.open("w") as f:
        json.dump(campaign.metadata, f, indent=2, default=str)


def _write_campaign_visualization(base_dir: Path, campaign: BenchmarkCampaign) -> None:
    campaign_viz_file = base_dir / "campaign_overview.pdf"
    plot_benchmark(campaign, file_path=campaign_viz_file, show_inline=False)


def _create_zip_archive(output_path: Path, base_dir: Path, final_path: Path) -> None:
    if final_path.exists():
        final_path.unlink()
    shutil.make_archive(
        str(output_path),
        "zip",
        root_dir=base_dir.parent,
        base_dir=base_dir.name,
    )


def _write_iterations(
    report_dir: Path,
    report: BenchmarkReport,
    pbar: ProgressBar,
) -> None:
    iterations_dir = report_dir / "iterations"
    iterations_dir.mkdir(exist_ok=True)

    for i, result in enumerate(report.results):
        iteration_file = iterations_dir / f"iteration_{i}.pdf"
        result.visualize(file_path=iteration_file, show_inline=False)
        pbar.update(1)


def _write_allocator_reports(
    source_dir: Path,
    allocator_name: str,
    variant_dict: dict[str, tuple[BenchmarkReport, ...]],
    visualize_iterations: bool,
    pbar: ProgressBar,
) -> None:
    allocator_dir = source_dir / allocator_name
    allocator_dir.mkdir(parents=True, exist_ok=True)

    for variant_label in sorted(variant_dict.keys()):
        reports = variant_dict[variant_label]
        variant_dir = allocator_dir / variant_label
        variant_dir.mkdir(parents=True, exist_ok=True)

        for report_idx, report in enumerate(reports):
            report_dir = (
                variant_dir / f"report_{report_idx}"
                if len(reports) > 1
                else variant_dir
            )
            report_dir.mkdir(parents=True, exist_ok=True)

            if visualize_iterations:
                _write_iterations(report_dir, report, pbar)
            else:
                pbar.update(1)


def _write_source_reports(
    base_dir: Path,
    source_name: str,
    allocator_dict: dict[str, dict[str, tuple[BenchmarkReport, ...]]],
    visualize_iterations: bool,
    pbar: ProgressBar,
) -> None:
    source_dir = base_dir / "sources" / source_name / "allocators"
    source_dir.mkdir(parents=True, exist_ok=True)

    for allocator_name in sorted(allocator_dict.keys()):
        _write_allocator_reports(
            source_dir,
            allocator_name,
            allocator_dict[allocator_name],
            visualize_iterations,
            pbar,
        )


def _write_nested_reports(
    base_dir: Path, campaign: BenchmarkCampaign, visualize_iterations: bool
) -> None:
    reports_by_source = campaign.reports_by_source_allocator_variant

    total_iterations = (
        sum(report.num_results for report in campaign.reports)
        if visualize_iterations
        else len(campaign.reports)
    )

    unit = "iteration" if visualize_iterations else "report"

    with tqdm(
        total=total_iterations,
        desc="Saving campaign",
        unit=unit,
        leave=False,
    ) as pbar:
        for source_name in sorted(reports_by_source.keys()):
            _write_source_reports(
                base_dir,
                source_name,
                reports_by_source[source_name],
                visualize_iterations,
                pbar,
            )


# TODO(fpedd): Add time stamp to campaign name optionally, to avoid overwriting
# existing campaigns.


def save_benchmark(
    campaign: BenchmarkCampaign | BenchmarkReport | BenchmarkResult,
    output_path: Path | str | None = None,
    output_format: Literal["dir", "zip"] = "dir",
    visualize_iterations: bool = True,
    overwrite: bool = True,
) -> Path:
    """Save benchmark campaign with optional visualization."""

    if not isinstance(campaign, BenchmarkCampaign):
        raise TypeError("save_benchmark only supports BenchmarkCampaign currently.")

    if output_path is None:
        output_path = PROJECT_DIR / "artifacts" / f"campaign_{campaign.id}"

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

    base_dir = _prepare_base_dir(output_path, output_format, overwrite)

    try:
        _write_metadata(base_dir, campaign)
        _write_campaign_visualization(base_dir, campaign)
        _write_nested_reports(base_dir, campaign, visualize_iterations)

        if output_format == "zip":
            _create_zip_archive(output_path, base_dir, final_path)
            logger.info(f"Campaign dumped to zip: {final_path}")
        else:
            logger.info(f"Campaign dumped to directory: {final_path}")

        return final_path

    finally:
        if output_format == "zip":
            shutil.rmtree(base_dir, ignore_errors=True)
