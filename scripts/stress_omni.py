#
# SPDX-License-Identifier: Apache-2.0
#
"""Stress the omni allocator across clock dimensions and concurrent callers.

Sweeps the two axes the other scripts hold fixed: clock dimension, and
concurrent callers checked against the serial placement, so races surface.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from math import isnan, nan
from pathlib import Path
from statistics import mean
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
from _plot import (
    AXIS,
    INK_SECONDARY,
    MARKERS,
    SERIES,
    SURFACE,
    log_time_axes,
    plot_series,
    style_axes,
    titles,
    two_panel_figure,
)
from omnimalloc.allocators import OmniAllocator
from omnimalloc.analysis import antichain_pressure, placement_pressure
from omnimalloc.benchmark.sources.sync_patterns import SYNC_PATTERNS, SyncPatternSource
from omnimalloc.benchmark.timer import Timer

if TYPE_CHECKING:
    from matplotlib.figure import Figure
    from omnimalloc.primitives import Allocation

DIMS = (2, 4, 8, 16, 32, 64)
PATTERNS = ("independent", "sparse", "barrier", "dense")
CALLERS = (1, 2, 4, 8, 16, 32)


def _timed_allocate(
    allocations: "tuple[Allocation, ...]", repeats: int
) -> "tuple[float, tuple[Allocation, ...]]":
    seconds = []
    placed: tuple[Allocation, ...] = ()
    for _ in range(repeats):
        with Timer() as timer:
            placed = OmniAllocator().allocate(allocations)
        seconds.append(timer.elapsed_s)
    return min(seconds), placed


def _bounded_ratio(
    allocations: "tuple[Allocation, ...]", peak: int, work_budget: int
) -> float:
    """Peak over the budgeted exact bound; NaN when the budget is exceeded."""
    try:
        return peak / antichain_pressure(allocations, work_budget=work_budget)
    except RuntimeError:
        return nan


def dim_sweep(args: argparse.Namespace) -> dict[str, dict[int, tuple[float, float]]]:
    results: dict[str, dict[int, tuple[float, float]]] = {}
    for pattern in args.patterns:
        results[pattern] = {}
        for dim in args.dims:
            times = []
            ratios = []
            for rep in range(args.repeats):
                allocations = SyncPatternSource(
                    num_allocations=args.size,
                    num_threads=dim,
                    pattern=pattern,
                    seed=args.seed + rep,
                ).get_allocations()
                seconds, placed = _timed_allocate(allocations, repeats=1)
                times.append(seconds)
                ratios.append(
                    _bounded_ratio(
                        allocations, placement_pressure(placed), args.work_budget
                    )
                )
            clean = [r for r in ratios if not isnan(r)]
            results[pattern][dim] = (min(times), mean(clean) if clean else nan)
            print(
                f"dim sweep: pattern={pattern} dim={dim} "
                f"time={min(times):.3f}s ratio={results[pattern][dim][1]:.3f}"
            )
    return results


def caller_sweep(args: argparse.Namespace) -> dict[int, float]:
    allocations = SyncPatternSource(
        num_allocations=args.size,
        num_threads=8,
        pattern="sparse",
        seed=args.seed,
    ).get_allocations()
    expected = [a.offset for a in OmniAllocator().allocate(allocations)]

    def run_once(_: int) -> list[int | None]:
        return [a.offset for a in OmniAllocator().allocate(allocations)]

    throughput: dict[int, float] = {}
    for callers in args.callers:
        with ThreadPoolExecutor(max_workers=callers) as executor, Timer() as timer:
            offsets = list(executor.map(run_once, range(args.calls)))
        if any(result != expected for result in offsets):
            raise AssertionError(f"non-deterministic placement with {callers} callers")
        throughput[callers] = args.calls / timer.elapsed_s
        print(
            f"caller sweep: callers={callers} wall={timer.elapsed_s:.2f}s "
            f"calls/s={throughput[callers]:.1f} "
            f"speedup={throughput[callers] / throughput[args.callers[0]]:.2f}x"
        )
    return throughput


def render_dim_sweep(
    results: dict[str, dict[int, tuple[float, float]]], args: argparse.Namespace
) -> "Figure":
    fig, ax_time, ax_ratio = two_panel_figure()
    dims = list(args.dims)
    for (pattern, by_dim), color, marker in zip(
        results.items(), SERIES, MARKERS, strict=False
    ):
        plot_series(ax_time, dims, [by_dim[d][0] for d in dims], pattern, color, marker)
        plot_series(
            ax_ratio, dims, [by_dim[d][1] for d in dims], pattern, color, marker
        )
    log_time_axes(ax_time, dims, base=2)
    titles(
        ax_time,
        "omni allocator vs clock dimension",
        f"{args.size:,} allocations, best of {args.repeats} seeds per point",
    )
    ax_time.legend(frameon=False, fontsize=8.5, labelcolor=INK_SECONDARY)
    style_axes(ax_ratio)
    ax_ratio.axhline(1.0, color=AXIS, linewidth=1)
    ax_ratio.set_ylabel("peak /\nexact bound", color=INK_SECONDARY, fontsize=9)
    ax_ratio.set_xlabel("clock dimension [threads]", color=INK_SECONDARY, fontsize=10)
    return fig


def render_caller_sweep(
    throughput: dict[int, float], args: argparse.Namespace
) -> "Figure":
    fig, ax = plt.subplots(figsize=(8, 4.4), layout="constrained")
    fig.set_facecolor(SURFACE)
    callers = list(throughput)
    ideal = [throughput[callers[0]] * c / callers[0] for c in callers]
    ax.plot(callers, ideal, color=AXIS, linewidth=1, linestyle="--", label="ideal")
    plot_series(
        ax, callers, [throughput[c] for c in callers], "measured", SERIES[0], "o"
    )
    style_axes(ax)
    ax.set_xscale("log", base=2)
    ax.set_xticks(callers, [str(c) for c in callers])
    ax.tick_params(which="minor", bottom=False)
    ax.set_ylabel("allocate calls / s", color=INK_SECONDARY, fontsize=10)
    ax.set_xlabel("concurrent Python callers", color=INK_SECONDARY, fontsize=10)
    titles(
        ax,
        "omni allocator throughput vs concurrent callers",
        f"{args.calls} calls on one {args.size:,}-allocation sparse instance, "
        "results checked against the serial placement",
    )
    ax.legend(frameon=False, fontsize=8.5, labelcolor=INK_SECONDARY, loc="upper left")
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--size", type=int, default=4_000)
    parser.add_argument("--dims", type=int, nargs="+", default=list(DIMS))
    parser.add_argument(
        "--patterns", nargs="+", choices=SYNC_PATTERNS, default=list(PATTERNS)
    )
    parser.add_argument("--callers", type=int, nargs="+", default=list(CALLERS))
    parser.add_argument("--calls", type=int, default=64)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--work-budget",
        type=int,
        default=4_000_000_000,
        help="pressure-bound work budget; exceeded points plot without a ratio",
    )
    parser.add_argument("--out", type=Path, default=Path("stress_results_omni"))
    args = parser.parse_args()

    results = dim_sweep(args)
    throughput = caller_sweep(args)

    args.out.mkdir(parents=True, exist_ok=True)
    figures = {
        "dim_sweep": render_dim_sweep(results, args),
        "caller_sweep": render_caller_sweep(throughput, args),
    }
    for name, figure in figures.items():
        figure.savefig(args.out / f"{name}.pdf")
        plt.close(figure)
    print(f"wrote figures to {args.out}/")


if __name__ == "__main__":
    main()
