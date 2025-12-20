#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from pathlib import Path
from typing import Any, Final

from omnimalloc.common.optional import require_optional

from .campaign import BenchmarkCampaign
from .report import BenchmarkReport
from .result import BenchmarkResult

try:
    import matplotlib.pyplot as plt
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure
    from matplotlib.lines import Line2D

    HAS_MATPLOTLIB = True
except ImportError:
    from types import SimpleNamespace

    HAS_MATPLOTLIB = False
    plt = SimpleNamespace(  # type: ignore[assignment]
        subplots=None,
        savefig=None,
        show=None,
        close=None,
    )
    Line2D = None  # type: ignore[assignment,misc]
    Axes = None  # type: ignore[assignment,misc]
    Figure = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

ALLOCATOR_COLORS: Final[tuple[str, ...]] = tuple(f"C{i}" for i in range(10))


def _get_allocator_color(index: int) -> str:
    return ALLOCATOR_COLORS[index % len(ALLOCATOR_COLORS)]


def _format_metadata(metadata: dict[str, Any] | None) -> str:
    if not metadata:
        return ""
    return " | ".join(
        f"{key.replace('_', ' ').title()}: {value}" for key, value in metadata.items()
    )


def _is_categorical(data: dict[str, dict[str, tuple[BenchmarkReport, ...]]]) -> bool:
    return any(
        data[name][alloc_name][0].is_categorical
        for name in data
        for alloc_name in data[name]
    )


def _get_sorted_reports(
    allocator_data: dict[str, tuple[BenchmarkReport, ...]],
) -> list[BenchmarkReport]:
    reports = [r for rs in allocator_data.values() for r in rs]
    is_categorical = any(r.is_categorical for r in reports)
    reports.sort(
        key=lambda r: (
            r.variant_id
            if is_categorical and r.variant_id is not None
            else r.num_allocations
        )
    )
    return reports


def _draw_graphs(
    ax: Axes,
    ax2: Axes,
    name: str,
    color: str,
    is_categorical: bool,
    reports: list[BenchmarkReport],
) -> None:
    x_vals = [
        (
            f"{r.variant_label}\n({r.num_allocations:,} allocations)"
            if is_categorical
            else r.num_allocations
        )
        for r in reports
    ]
    times = [r.average_seconds for r in reports]
    efficiencies = [r.average_allocation_efficiency * 100 for r in reports]

    ax.plot(
        x_vals,
        times,
        marker="o",
        linestyle="-",
        linewidth=2,
        markersize=6,
        label=name,
        color=color,
        alpha=0.8,
    )

    ax2.plot(
        x_vals,
        efficiencies,
        marker="s",
        linestyle="--",
        linewidth=1.5,
        markersize=4,
        color=color,
        alpha=0.4,
    )

    for i, (x, y) in enumerate(zip(x_vals, times, strict=True)):
        ax.text(
            i if is_categorical else float(x),
            y,
            f"{y:.3f}s",
            ha="center",
            va="bottom",
            fontsize=7,
            color=color,
            fontweight="bold",
            bbox={
                "boxstyle": "round,pad=0.3",
                "facecolor": "white",
                "edgecolor": color,
                "alpha": 0.8,
                "linewidth": 0.5,
            },
        )

    for i, (x, y) in enumerate(zip(x_vals, efficiencies, strict=True)):
        ax2.text(
            i if is_categorical else float(x),
            y,
            f"{y:.1f}%",
            ha="center",
            va="bottom",
            fontsize=7,
            color=color,
            alpha=0.6,
            bbox={
                "boxstyle": "round,pad=0.3",
                "facecolor": "white",
                "edgecolor": color,
                "alpha": 0.5,
                "linewidth": 0.5,
            },
        )


def _draw_subplot(
    ax: Axes,
    source_name: str,
    source_data: dict[str, dict[str, tuple[BenchmarkReport, ...]]],
) -> None:
    ax2 = ax.twinx()

    is_categorical = _is_categorical(source_data)

    for i, (allocator_name, allocator_data) in enumerate(source_data.items()):
        color = _get_allocator_color(i)
        reports = _get_sorted_reports(allocator_data)

        _draw_graphs(ax, ax2, allocator_name, color, is_categorical, reports)

    if is_categorical:
        ax.set_xlabel("Model / Variant", fontsize=10)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
    else:
        ax.set_xlabel("Number of Allocations", fontsize=10)
        num_allocations = [r.num_allocations for r in reports]
        if num_allocations and max(num_allocations) / min(num_allocations) > 10:
            ax.set_xscale("log")

    ax.set_ylabel("Time (s)", fontsize=10, color="black")
    ax.tick_params(axis="y", labelcolor="black")

    ax2.set_ylabel("Efficiency (%)", fontsize=10, color="gray")
    ax2.tick_params(axis="y", labelcolor="gray")
    ax2.set_ylim(0, 105)

    ax.grid(visible=True, alpha=0.3, linestyle="--")
    ax.set_title(f"Source: {source_name}", fontsize=12, fontweight="bold", pad=10)


def _add_footer(campaign: BenchmarkCampaign, fig: Figure) -> None:
    metadata_text = _format_metadata(campaign.metadata)
    txt = fig.text(
        0.5,
        0.02,
        metadata_text,
        ha="center",
        va="bottom",
        fontsize=8,
        color="#555555",
        wrap=True,
    )
    txt._get_wrap_line_width = lambda: fig.bbox.width * 0.90  # type: ignore[attr-defined]  # noqa: SLF001


def _add_legend(fig: Figure, allocator_names: tuple[str, ...]) -> None:
    handles = [
        Line2D(
            [],
            [],
            color=_get_allocator_color(i),
            marker="o",
            linestyle="-",
            linewidth=2,
            markersize=6,
            label=name,
        )
        for i, name in enumerate(allocator_names)
    ]

    fig.legend(
        handles=handles,
        loc="outside upper center",
        ncol=len(handles),
        fontsize=8,
        title="Allocators",
    )


def _create_figure(num_sources: int) -> tuple[Figure, list[Axes]]:
    fig, axs = plt.subplots(
        nrows=num_sources,
        ncols=1,
        figsize=(14, max(6, num_sources * 6)),
    )
    return fig, [axs] if num_sources == 1 else axs


def _visualize_campaign(
    campaign: BenchmarkCampaign,
    file_path: Path | str | None,
    show_inline: bool,
) -> Path | None:
    source_names = campaign.source_names
    allocator_names = campaign.allocator_names
    reports_by_source = campaign.reports_by_source_allocator_variant

    if not source_names:
        raise ValueError("Campaign has no sources to visualize")
    if not allocator_names:
        raise ValueError("Campaign has no allocators to visualize")
    if not reports_by_source:
        raise ValueError("Campaign has no reports to visualize")

    fig, axs = _create_figure(len(source_names))

    for ax, source_name in zip(axs, source_names, strict=True):
        _draw_subplot(ax, source_name, reports_by_source[source_name])

    fig.tight_layout(rect=(0.01, 0.05, 0.99, 0.92))  # l, b, r, t

    _add_footer(campaign, fig)
    _add_legend(fig, allocator_names)

    if show_inline:
        plt.show()

    if file_path is not None:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(file_path, bbox_inches="tight", format="pdf")
        logger.info(f"Visualization saved to {file_path}")

    plt.close(fig)

    return file_path


def _canonicalize_artifact(
    artifact: BenchmarkResult | BenchmarkReport | BenchmarkCampaign,
) -> BenchmarkCampaign:
    if isinstance(artifact, BenchmarkResult):
        artifact = BenchmarkReport(id=f"report_{artifact.id}", results=(artifact,))
    if isinstance(artifact, BenchmarkReport):
        artifact = BenchmarkCampaign(id=f"campaign_{artifact.id}", reports=(artifact,))
    return artifact


def plot_benchmark(
    artifact: BenchmarkResult | BenchmarkReport | BenchmarkCampaign,
    file_path: Path | str | None = None,
    show_inline: bool = False,
) -> Path | None:
    """Visualize benchmark results.

    Args:
        artifact: The benchmark artifact to visualize.
        file_path: Optional path to save the plot.
        show_inline: Whether to display inline (for notebooks).

    Returns:
        Path to the saved file, or None if not saved.
    """
    if not HAS_MATPLOTLIB:
        require_optional("matplotlib", "benchmark visualization")

    campaign = _canonicalize_artifact(artifact)
    return _visualize_campaign(campaign, file_path, show_inline)
