#
# SPDX-License-Identifier: Apache-2.0
#
"""Benchmark and torture-test allocators on vector-clock workloads.

Sweeps problem size over two workload families and renders one figure per
family, each with wall time on top and packing quality below:

- ``SyncPatternSource``: every sync pattern (loosest to tightest coupling)
- ``ConcurrentTilingSource``: a ``num_syncs`` sweep with a provably
  achievable optimum (``capacity``), so quality is measured against ground
  truth instead of best-of-run

Any allocator whose single run exceeds ``--budget`` seconds is dropped for
the rest of the sweep; partially sampled points are not plotted, so every
plotted point averages the full workload mix. Placements up to
``VALIDATE_LIMIT`` allocations are checked with ``validate_allocation``
(pairwise, quadratic) and larger ones against peak bounds; any violation
aborts the run, so the sweep doubles as a fuzz/torture pass.

    uv run python scripts/benchmark_allocation.py --out benchmark_results_allocation
"""

from __future__ import annotations

import argparse
from math import isnan, nan
from pathlib import Path
from statistics import mean
from typing import TYPE_CHECKING, Any

import matplotlib.pyplot as plt
from omnimalloc.allocators import BaseAllocator
from omnimalloc.benchmark.sources.concurrent_tiling import ConcurrentTilingSource
from omnimalloc.benchmark.sources.sync_patterns import SYNC_PATTERNS, SyncPatternSource
from omnimalloc.benchmark.timer import Timer
from omnimalloc.primitives import Pool
from omnimalloc.validate import validate_allocation

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure
    from omnimalloc.primitives import Allocation

SIZES = (1_000, 3_000, 10_000, 30_000, 100_000)
NUM_SYNCS = (0, 64, 1_024)
# validate_allocation checks all pairs in Python; cap exact validation and
# fall back to peak-bound sanity checks on larger instances
VALIDATE_LIMIT = 2_000
ALLOCATORS = (
    "omni",
    "greedy_by_all",
    "greedy",
    "best_fit",
    "naive",
)

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SERIES = ("#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7", "#e34948", "#e87ba4")

Sample = dict[str, Any]


def _timed(
    allocator: BaseAllocator, allocations: tuple[Allocation, ...]
) -> tuple[float, tuple[Allocation, ...]]:
    timer = Timer(auto_start=True)
    placed = allocator.allocate(allocations)
    timer.stop()
    seconds = timer.elapsed_s
    if seconds < 1e-3:
        for _ in range(4):
            timer = Timer(auto_start=True)
            allocator.allocate(allocations)
            timer.stop()
            seconds = min(seconds, timer.elapsed_s)
    return seconds, placed


def _peak(placed: tuple[Allocation, ...]) -> int:
    heights = [alloc.height for alloc in placed if alloc.height is not None]
    return max(heights, default=0)


def _run_allocators(
    allocations: tuple[Allocation, ...],
    allocators: dict[str, BaseAllocator],
    dropped: set[str],
    budget: float,
    context: str,
) -> Sample:
    """Time every allocator not yet dropped; validate each placement."""
    sample: Sample = {"times": {}, "peaks": {}}
    for name, allocator in allocators.items():
        if name in dropped or not allocator.supports(allocations):
            continue
        seconds, placed = _timed(allocator, allocations)
        if len(placed) <= VALIDATE_LIMIT:
            validate_allocation(Pool(id=context, allocations=placed))
        else:
            assert all(a.offset is not None and a.offset >= 0 for a in placed)
            assert _peak(placed) <= sum(a.size for a in placed)
        sample["times"][name] = seconds
        sample["peaks"][name] = _peak(placed)
        if seconds > budget:
            dropped.add(name)
            print(f"dropping {name} after {_fmt(seconds)} at {context}")
    return sample


def _finish_sample(sample: Sample, optimum: int | None) -> Sample:
    """Attach per-allocator quality ratios (vs ground truth or best-of-run)."""
    peaks = sample["peaks"]
    reference = optimum if optimum is not None else min(peaks.values(), default=0)
    # Always set ratios: a sample where every allocator was skipped or dropped
    # has no reference, and the summary/plot lookups expect the key
    sample["ratios"] = (
        {name: peak / reference for name, peak in peaks.items()} if reference else {}
    )
    return sample


def collect(args: argparse.Namespace) -> list[Sample]:
    allocators = {name: BaseAllocator.get(name)() for name in args.allocators}
    samples: list[Sample] = []
    dropped: set[str] = set()
    for size in args.sizes:
        for pattern in args.patterns:
            for rep in range(args.repeats):
                seed = args.seed + rep
                allocations = SyncPatternSource(
                    num_allocations=size,
                    num_threads=args.threads,
                    pattern=pattern,
                    seed=seed,
                ).get_allocations()
                context = f"pattern={pattern} size={size} seed={seed}"
                sample = _run_allocators(
                    allocations, allocators, dropped, args.budget, context
                )
                sample.update(family="sync", size=size)
                samples.append(_finish_sample(sample, optimum=None))
        for num_syncs in args.num_syncs:
            for rep in range(args.repeats):
                seed = args.seed + rep
                source = ConcurrentTilingSource(
                    num_allocations=size,
                    num_threads=args.threads,
                    num_syncs=num_syncs,
                    seed=seed,
                )
                context = f"tiling syncs={num_syncs} size={size} seed={seed}"
                sample = _run_allocators(
                    source.get_allocations(), allocators, dropped, args.budget, context
                )
                sample.update(family="tiling", size=size)
                samples.append(_finish_sample(sample, optimum=source.capacity))
        print(f"size {size} done ({len(samples)} instances total)")
    return samples


def _series_means(
    samples: list[Sample],
    sizes: list[int],
    family: str,
    name: str,
    key: str,
    expected: int,
) -> list[float]:
    means = []
    for size in sizes:
        values = [
            s[key][name]
            for s in samples
            if s["family"] == family and s["size"] == size and name in s[key]
        ]
        means.append(mean(values) if len(values) >= expected else nan)
    return means


def _fmt(seconds: float) -> str:
    if isnan(seconds):
        return "-"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.0f} us"
    if seconds < 1.0:
        return f"{seconds * 1e3:.1f} ms"
    return f"{seconds:.2f} s"


def _print_summary(samples: list[Sample], args: argparse.Namespace) -> None:
    expected = {
        "sync": len(args.patterns) * args.repeats,
        "tiling": len(args.num_syncs) * args.repeats,
    }
    width = max(len(name) for name in args.allocators)
    for family, family_expected in expected.items():
        print(f"\n{family} wall time / quality (peak over optimum or best-of-run)")
        print(f"{'':<{width}}  " + "".join(f"{size:>16}" for size in args.sizes))
        for name in args.allocators:
            times = _series_means(
                samples, args.sizes, family, name, "times", family_expected
            )
            ratios = _series_means(samples, args.sizes, family, name, "ratios", 1)
            cells = [
                f"{_fmt(t)} {'-' if isnan(r) else f'{r:.3f}'}"
                for t, r in zip(times, ratios, strict=True)
            ]
            print(f"{name:<{width}}  " + "".join(f"{cell:>16}" for cell in cells))
    print()


def _style_axes(ax: Axes) -> None:
    ax.set_facecolor(SURFACE)
    ax.grid(visible=True, which="major", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(colors=INK_MUTED, labelcolor=INK_SECONDARY, labelsize=9)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("bottom", "left"):
        ax.spines[side].set_color(AXIS)


def _plot_series(
    ax: Axes, sizes: list[int], values: list[float], label: str, color: str
) -> None:
    # An all-NaN series (allocator skipped or dropped everywhere) would leave
    # the log-scaled axis without positive values and crash on save
    if all(isnan(value) for value in values):
        return
    ax.plot(
        sizes,
        values,
        label=label,
        color=color,
        linewidth=2,
        marker="o",
        markersize=5.5,
        markeredgecolor=SURFACE,
        markeredgewidth=1,
    )


def _finish_time_axes(ax: Axes, sizes: list[int]) -> None:
    _style_axes(ax)
    ax.set_yscale("log")
    ax.set_xscale("log")
    ax.set_xticks(sizes, [f"{size:,}" for size in sizes])
    ax.tick_params(which="minor", bottom=False)
    ax.set_ylabel("wall time [s]", color=INK_SECONDARY, fontsize=10)


def _titles(ax: Axes, title: str, caption: str) -> None:
    ax.set_title(title, loc="left", color=INK, fontsize=12, fontweight="medium", pad=22)
    note = ax.text(
        0.0, 1.04, caption, transform=ax.transAxes, fontsize=8.5, color=INK_MUTED
    )
    note.set_in_layout(False)


def _legend(ax: Axes) -> None:
    ax.legend(frameon=False, fontsize=8.5, labelcolor=INK_SECONDARY, loc="upper left")


def _render_family(
    samples: list[Sample],
    args: argparse.Namespace,
    family: str,
    title: str,
    caption: str,
    quality_label: str,
    expected: int,
) -> Figure:
    fig, (ax_time, ax_ratio) = plt.subplots(
        2,
        1,
        sharex=True,
        figsize=(8, 6.4),
        height_ratios=(3, 1.4),
        layout="constrained",
    )
    fig.set_facecolor(SURFACE)
    for name, color in zip(args.allocators, SERIES, strict=False):
        times = _series_means(samples, args.sizes, family, name, "times", expected)
        _plot_series(ax_time, args.sizes, times, name, color)
        ratios = _series_means(samples, args.sizes, family, name, "ratios", 1)
        _plot_series(ax_ratio, args.sizes, ratios, name, color)
    _finish_time_axes(ax_time, args.sizes)
    _titles(ax_time, title, caption)
    _legend(ax_time)
    _style_axes(ax_ratio)
    ax_ratio.axhline(1.0, color=AXIS, linewidth=1)
    ax_ratio.set_ylabel(quality_label, color=INK_SECONDARY, fontsize=9)
    ax_ratio.set_xlabel("problem size [allocations]", color=INK_SECONDARY, fontsize=10)
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sizes", type=int, nargs="+", default=list(SIZES))
    parser.add_argument(
        "--patterns", nargs="+", choices=SYNC_PATTERNS, default=list(SYNC_PATTERNS)
    )
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--num-syncs", type=int, nargs="+", default=list(NUM_SYNCS))
    parser.add_argument("--allocators", nargs="+", default=list(ALLOCATORS))
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--budget", type=float, default=10.0, help="per-run drop threshold [s]"
    )
    parser.add_argument(
        "--out", type=Path, default=Path("benchmark_results_allocation")
    )
    args = parser.parse_args()

    samples = collect(args)
    _print_summary(samples, args)

    args.out.mkdir(parents=True, exist_ok=True)
    figures = {
        "sync_patterns": _render_family(
            samples,
            args,
            family="sync",
            title="allocators on sync-pattern workloads",
            caption=(
                f"each point: mean over {len(args.patterns)} sync patterns x "
                f"{args.repeats} seeds, {args.threads} threads; "
                "partial points omitted"
            ),
            quality_label="peak /\nbest-of-run",
            expected=len(args.patterns) * args.repeats,
        ),
        "concurrent_tiling": _render_family(
            samples,
            args,
            family="tiling",
            title="allocators on concurrent-tiling workloads",
            caption=(
                f"each point: mean over num_syncs in {args.num_syncs} x "
                f"{args.repeats} seeds, {args.threads} threads; "
                "quality vs provably achievable optimum"
            ),
            quality_label="peak /\noptimum",
            expected=len(args.num_syncs) * args.repeats,
        ),
    }
    for name, figure in figures.items():
        figure.savefig(args.out / f"{name}.pdf")
        plt.close(figure)
    print(f"wrote figures to {args.out}/")


if __name__ == "__main__":
    main()
