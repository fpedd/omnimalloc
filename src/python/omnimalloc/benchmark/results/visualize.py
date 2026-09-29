#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

from omnimalloc.common.optional import require_optional

from .campaign import BenchmarkCampaign
from .report import BenchmarkReport
from .result import BenchmarkResult

try:
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    plt = Line2D = cast("Any", None)

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

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


def _sorted_reports(reports: list[BenchmarkReport]) -> list[BenchmarkReport]:
    # One key type for the whole group: mixing a categorical variant_id with a
    # numeric fallback compares str against int and raises mid-sort
    if any(r.is_categorical for r in reports):
        return sorted(reports, key=lambda r: str(r.variant_id))
    return sorted(reports, key=lambda r: r.num_allocations)


def _draw_graphs(
    ax: "Axes",
    ax2: "Axes",
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
    times = [r.mean_seconds for r in reports]

    # Efficiency is unknown wherever the pressure analysis gave up; those
    # points drop out of the series instead of breaking the whole plot
    efficiencies: list[tuple[int, Any, float]] = []
    for index, (x, report) in enumerate(zip(x_vals, reports, strict=True)):
        efficiency = report.mean_allocation_efficiency
        if efficiency is not None:
            efficiencies.append((index, x, efficiency * 100))

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
        [x for _, x, _ in efficiencies],
        [y for _, _, y in efficiencies],
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

    for i, x, y in efficiencies:
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
    ax: "Axes",
    source_name: str,
    reports: list[BenchmarkReport],
    allocator_names: tuple[str, ...],
) -> None:
    ax2 = ax.twinx()
    is_categorical = any(r.is_categorical for r in reports)

    # Color by campaign-wide allocator index so colors match the legend
    # even when a source lacks some allocators.
    for index, allocator_name in enumerate(allocator_names):
        series = [r for r in reports if r.allocator_name == allocator_name]
        if series:
            color = _get_allocator_color(index)
            _draw_graphs(
                ax, ax2, allocator_name, color, is_categorical, _sorted_reports(series)
            )

    if is_categorical:
        ax.set_xlabel("Model / Variant", fontsize=10)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
    else:
        ax.set_xlabel("Number of Allocations", fontsize=10)
        num_allocations = [r.num_allocations for r in reports]
        if max(num_allocations) / min(num_allocations) > 10:
            ax.set_xscale("log")

    ax.set_ylabel("Time (s)", fontsize=10, color="black")
    ax.tick_params(axis="y", labelcolor="black")

    ax2.set_ylabel("Efficiency (%)", fontsize=10, color="gray")
    ax2.tick_params(axis="y", labelcolor="gray")
    ax2.set_ylim(0, 105)

    ax.grid(visible=True, alpha=0.3, linestyle="--")
    ax.set_title(f"Source: {source_name}", fontsize=12, fontweight="bold", pad=10)


def _add_footer(campaign: BenchmarkCampaign, fig: "Figure") -> None:
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
    txt._get_wrap_line_width = lambda: fig.bbox.width * 0.90  # ty: ignore[unresolved-attribute]  # noqa: SLF001


def _add_legend(fig: "Figure", allocator_names: tuple[str, ...]) -> None:
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


def _create_figure(num_sources: int) -> "tuple[Figure, list[Axes]]":
    fig, axs = plt.subplots(
        nrows=num_sources,
        ncols=1,
        figsize=(14, max(6, num_sources * 6)),
    )
    return fig, [axs] if num_sources == 1 else axs


def _visualize_campaign(
    campaign: BenchmarkCampaign,
    path: Path | str | None,
) -> None:
    source_names = campaign.source_names
    allocator_names = campaign.allocator_names
    fig, axs = _create_figure(len(source_names))

    for ax, source_name in zip(axs, source_names, strict=True):
        reports = [r for r in campaign.reports if r.source_name == source_name]
        _draw_subplot(ax, source_name, reports, allocator_names)

    fig.tight_layout(rect=(0.01, 0.05, 0.99, 0.92))  # l, b, r, t

    _add_footer(campaign, fig)
    _add_legend(fig, allocator_names)

    if path is None:
        plt.show()
    else:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, bbox_inches="tight", format="pdf")
        logger.info(f"Visualization saved to {path}")

    plt.close(fig)


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
    path: Path | str | None = None,
) -> None:
    """Plot a benchmark artifact: `path=None` displays the figure, `path=...` saves it.

    Raises `ImportError` without matplotlib.
    """
    if not HAS_MATPLOTLIB:
        require_optional("matplotlib", "benchmark visualization")

    _visualize_campaign(_canonicalize_artifact(artifact), path)
