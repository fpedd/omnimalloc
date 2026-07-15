#
# SPDX-License-Identifier: Apache-2.0
#
"""Benchmark exact pressure lower bounds on vector-clock workloads.

Compares the shipping ``get_pressure`` (linearize then sweep, else the exact
antichain) against the two exact C++ methods:

- ``get_antichain_pressure``: max-weight antichain (weighted Dilworth via
  min flow), the tightest sound lower bound on any placement's peak and the
  reference every ratio is measured against
- ``get_closure_pressure``: realizable peak via join-closure enumeration;
  instances whose closure exceeds ``--closure-cap`` are reported as capped
  and excluded from the means. Instances where ``get_pressure`` exceeds its
  default work budget are reported the same way

and their per-allocation counterparts (dashed in the figures; ratios are
means over the per-allocation pinned antichain):

- ``get_per_allocation_pressure``: pinned antichain per distinct lifetime
- ``get_per_allocation_closure_pressure``: realizable peak per allocation
- ``get_per_allocation_placement_pressure``: placement-certified bound read
  off an untimed ``OmniAllocator`` placement, plain and ``clique_cap`` forms

Each sample also places the workload with ``OmniAllocator`` and reports its
peak over the antichain bound, so a ratio of 1.000 certifies the placement
optimal. Every sample cross-checks the bound order — globally ``get_pressure``
equal to the antichain, closure at or below it, omni peak at or above it,
antichain at or below the tiling optimum; per allocation closure <= pinned
antichain <= clique-capped <= plain placement, with each per-allocation max
matching its global counterpart and the plain placement max matching the
placement's peak — so the sweep doubles as a torture pass for the exact
primitives. Any method whose single run exceeds ``--budget`` seconds is
dropped for the rest of the sweep.

    uv run python scripts/benchmark_pressure.py --out benchmark_results_pressure
"""

from __future__ import annotations

import argparse
from math import isnan, nan
from pathlib import Path
from statistics import mean
from typing import TYPE_CHECKING, Any

import matplotlib.pyplot as plt
from omnimalloc import OmniAllocator
from omnimalloc.benchmark.sources.concurrent_tiling import ConcurrentTilingSource
from omnimalloc.benchmark.sources.sync_patterns import SYNC_PATTERNS, SyncPatternSource
from omnimalloc.benchmark.timer import Timer
from omnimalloc.primitives import (
    get_antichain_pressure,
    get_closure_pressure,
    get_per_allocation_closure_pressure,
    get_per_allocation_placement_pressure,
    get_per_allocation_pressure,
)
from omnimalloc.primitives.pressure import get_pressure

if TYPE_CHECKING:
    from collections.abc import Callable

    from matplotlib.axes import Axes
    from matplotlib.figure import Figure
    from omnimalloc.primitives import Allocation, IdType

    Value = int | dict["IdType", int] | None
    Runner = Callable[[], "Value"]

SIZES = (100, 300, 1_000, 3_000, 10_000)
NUM_SYNCS = (0, 64, 1_024)
REFERENCE = "get_antichain_pressure"
PER_ALLOCATION_REFERENCE = "get_per_allocation_pressure"
METHODS = (
    "get_pressure",
    REFERENCE,
    "get_closure_pressure",
    "omni_allocator",
    PER_ALLOCATION_REFERENCE,
    "get_per_allocation_closure_pressure",
    "get_per_allocation_placement_pressure",
    "placement_clique_cap",
)
DEFAULT_CLOSURE_CAP = 1 << 16

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
COLORS = {
    "get_pressure": "#2a78d6",
    "get_antichain_pressure": "#1baf7a",
    "get_closure_pressure": "#eda100",
    "omni_allocator": "#4a3aa7",
    "get_per_allocation_pressure": "#1baf7a",
    "get_per_allocation_closure_pressure": "#eda100",
    "get_per_allocation_placement_pressure": "#4a3aa7",
    "placement_clique_cap": "#4a3aa7",
}
# Per-allocation variants share their global counterpart's color, dashed
LINESTYLES = {
    "get_per_allocation_pressure": "--",
    "get_per_allocation_closure_pressure": "--",
    "get_per_allocation_placement_pressure": "--",
    "placement_clique_cap": ":",
}

Sample = dict[str, Any]


def _peak(placed: tuple[Allocation, ...]) -> int:
    heights = [alloc.height for alloc in placed if alloc.height is not None]
    return max(heights, default=0)


def _capped(
    query: Callable[..., Any], allocations: tuple[Allocation, ...], cap: int
) -> Value:
    """None instead of raising when the join closure exceeds the cap."""
    try:
        return query(allocations, closure_cap=cap)
    except RuntimeError:
        return None


def _budgeted(allocations: tuple[Allocation, ...]) -> Value:
    """None instead of raising when get_pressure exceeds its work budget."""
    try:
        return get_pressure(allocations)
    except RuntimeError:
        return None


def _sample_runners(
    allocations: tuple[Allocation, ...],
    placed: tuple[Allocation, ...],
    allocator: OmniAllocator,
    args: argparse.Namespace,
) -> dict[str, Runner]:
    return {
        "get_pressure": lambda: _budgeted(allocations),
        REFERENCE: lambda: get_antichain_pressure(allocations),
        "get_closure_pressure": lambda: _capped(
            get_closure_pressure, allocations, args.closure_cap
        ),
        "omni_allocator": lambda: _peak(allocator.allocate(allocations)),
        PER_ALLOCATION_REFERENCE: lambda: get_per_allocation_pressure(allocations),
        "get_per_allocation_closure_pressure": lambda: _capped(
            get_per_allocation_closure_pressure, allocations, args.closure_cap
        ),
        "get_per_allocation_placement_pressure": lambda: (
            get_per_allocation_placement_pressure(placed)
        ),
        "placement_clique_cap": lambda: (
            get_per_allocation_placement_pressure(placed, clique_cap=True)
        ),
    }


def _timed(runner: Runner) -> tuple[float, Value]:
    timer = Timer(auto_start=True)
    value = runner()
    timer.stop()
    seconds = timer.elapsed_s
    if seconds < 1e-3:
        for _ in range(4):
            timer = Timer(auto_start=True)
            runner()
            timer.stop()
            seconds = min(seconds, timer.elapsed_s)
    return seconds, value


def _run_methods(
    runners: dict[str, Runner],
    dropped: set[str],
    capped: dict[str, dict[int, int]],
    size: int,
    budget: float,
    context: str,
) -> Sample:
    """Time every method not yet dropped; capped closures record nothing."""
    sample: Sample = {"times": {}, "values": {}}
    for name, runner in runners.items():
        if name in dropped:
            continue
        seconds, value = _timed(runner)
        if value is None:
            counts = capped.setdefault(name, {})
            counts[size] = counts.get(size, 0) + 1
        else:
            sample["times"][name] = seconds
            sample["values"][name] = value
        if seconds > budget:
            dropped.add(name)
            print(f"dropping {name} after {_fmt(seconds)} at {context}")
    return sample


def _finish_sample(
    sample: Sample, placed: tuple[Allocation, ...], optimum: int | None
) -> Sample:
    """Cross-check the bound order and identities, attach ratios."""
    values = sample.pop("values")
    reference = values.get(REFERENCE)
    if not reference:
        return sample
    assert values.get("get_pressure", reference) == reference
    assert values.get("get_closure_pressure", 0) <= reference
    assert values.get("omni_allocator", reference) >= reference
    assert optimum is None or reference <= optimum
    _check_per_allocation(values, reference, placed)
    sample["ratios"] = _ratios(values, reference)
    return sample


def _check_per_allocation(
    values: dict[str, Any], reference: int, placed: tuple[Allocation, ...]
) -> None:
    """Per-allocation identities: maxima match globals, bounds are ordered."""
    per_alloc = values.get(PER_ALLOCATION_REFERENCE)
    closure = values.get("get_per_allocation_closure_pressure")
    placement = values.get("get_per_allocation_placement_pressure")
    clique = values.get("placement_clique_cap")
    if per_alloc:
        assert max(per_alloc.values()) == reference
        for i, pinned in per_alloc.items():
            low = closure[i] if closure else pinned
            mid = clique[i] if clique else pinned
            high = placement[i] if placement else mid
            assert low <= pinned <= mid <= high
    if closure and values.get("get_closure_pressure") is not None:
        assert max(closure.values()) == values["get_closure_pressure"]
    if placement:
        assert max(placement.values()) == _peak(placed)


def _ratios(values: dict[str, Any], reference: int) -> dict[str, float]:
    """Global ratios over the antichain; per-allocation dicts as mean ratios."""
    per_alloc = values.get(PER_ALLOCATION_REFERENCE)
    ratios: dict[str, float] = {}
    for name, value in values.items():
        if isinstance(value, dict):
            if per_alloc:
                ratios[name] = mean(value[i] / per_alloc[i] for i in per_alloc)
        else:
            ratios[name] = value / reference
    return ratios


def collect(args: argparse.Namespace) -> tuple[list[Sample], dict[str, dict[int, int]]]:
    allocator = OmniAllocator()
    samples: list[Sample] = []
    dropped: set[str] = set()
    capped: dict[str, dict[int, int]] = {}
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
                placed = allocator.allocate(allocations)
                context = f"pattern={pattern} size={size} seed={seed}"
                sample = _run_methods(
                    _sample_runners(allocations, placed, allocator, args),
                    dropped,
                    capped,
                    size,
                    args.budget,
                    context,
                )
                sample = _finish_sample(sample, placed, optimum=None)
                sample.update(family="sync", size=size)
                samples.append(sample)
        for num_syncs in args.num_syncs:
            for rep in range(args.repeats):
                seed = args.seed + rep
                source = ConcurrentTilingSource(
                    num_allocations=size,
                    num_threads=args.threads,
                    num_syncs=num_syncs,
                    seed=seed,
                )
                allocations = source.get_allocations()
                placed = allocator.allocate(allocations)
                context = f"tiling syncs={num_syncs} size={size} seed={seed}"
                sample = _run_methods(
                    _sample_runners(allocations, placed, allocator, args),
                    dropped,
                    capped,
                    size,
                    args.budget,
                    context,
                )
                sample = _finish_sample(sample, placed, optimum=source.capacity)
                sample.update(family="tiling", size=size)
                samples.append(sample)
        print(f"size {size} done ({len(samples)} instances total)")
    return samples, capped


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
            if s["family"] == family and s["size"] == size and name in s.get(key, {})
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


def _print_summary(
    samples: list[Sample], capped: dict[str, dict[int, int]], args: argparse.Namespace
) -> None:
    expected = {
        "sync": len(args.patterns) * args.repeats,
        "tiling": len(args.num_syncs) * args.repeats,
    }
    width = max(len(name) for name in METHODS)
    for family, family_expected in expected.items():
        print(
            f"\n{family} wall time / value over antichain "
            "(per-allocation: mean, omni: peak)"
        )
        print(f"{'':<{width}}  " + "".join(f"{size:>16}" for size in args.sizes))
        for name in METHODS:
            times = _series_means(
                samples, args.sizes, family, name, "times", family_expected
            )
            ratios = _series_means(samples, args.sizes, family, name, "ratios", 1)
            cells = [
                f"{_fmt(t)} {'-' if isnan(r) else f'{r:.3f}'}"
                for t, r in zip(times, ratios, strict=True)
            ]
            print(f"{name:<{width}}  " + "".join(f"{cell:>16}" for cell in cells))
    for name, counts in sorted(capped.items()):
        joined = ", ".join(f"{size}: {count}" for size, count in sorted(counts.items()))
        print(f"\n{name} capped instances per size ({joined})")
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


def _plot_series(ax: Axes, sizes: list[int], values: list[float], name: str) -> None:
    ax.plot(
        sizes,
        values,
        label=name,
        color=COLORS[name],
        linestyle=LINESTYLES.get(name, "-"),
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


def _render_family(
    samples: list[Sample],
    args: argparse.Namespace,
    family: str,
    title: str,
    caption: str,
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
    for name in METHODS:
        times = _series_means(samples, args.sizes, family, name, "times", expected)
        _plot_series(ax_time, args.sizes, times, name)
        if name not in (REFERENCE, PER_ALLOCATION_REFERENCE):
            ratios = _series_means(samples, args.sizes, family, name, "ratios", 1)
            _plot_series(ax_ratio, args.sizes, ratios, name)
    _finish_time_axes(ax_time, args.sizes)
    _titles(ax_time, title, caption)
    ax_time.legend(
        frameon=False,
        fontsize=8,
        labelcolor=INK_SECONDARY,
        loc="upper left",
        ncols=2,
    )
    _style_axes(ax_ratio)
    ax_ratio.axhline(1.0, color=COLORS[REFERENCE], linewidth=1)
    ax_ratio.set_ylabel("value /\nantichain", color=INK_SECONDARY, fontsize=9)
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
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--closure-cap", type=int, default=DEFAULT_CLOSURE_CAP)
    parser.add_argument(
        "--budget", type=float, default=10.0, help="per-run drop threshold [s]"
    )
    parser.add_argument("--out", type=Path, default=Path("benchmark_results_pressure"))
    args = parser.parse_args()

    samples, capped = collect(args)
    _print_summary(samples, capped, args)

    args.out.mkdir(parents=True, exist_ok=True)
    figures = {
        "sync_patterns": _render_family(
            samples,
            args,
            family="sync",
            title="pressure lower bounds on sync-pattern workloads",
            caption=(
                f"each point: mean over {len(args.patterns)} sync patterns x "
                f"{args.repeats} seeds, {args.threads} threads; ratios vs the "
                "exact antichain (per-allocation: mean vs the pinned antichain); "
                "capped/partial points omitted"
            ),
            expected=len(args.patterns) * args.repeats,
        ),
        "concurrent_tiling": _render_family(
            samples,
            args,
            family="tiling",
            title="pressure lower bounds on concurrent-tiling workloads",
            caption=(
                f"each point: mean over num_syncs in {args.num_syncs} x "
                f"{args.repeats} seeds, {args.threads} threads; ratios vs the "
                "exact antichain (per-allocation: mean vs the pinned antichain); "
                "capped/partial points omitted"
            ),
            expected=len(args.num_syncs) * args.repeats,
        ),
    }
    for name, figure in figures.items():
        figure.savefig(args.out / f"{name}.pdf")
        plt.close(figure)
    print(f"wrote figures to {args.out}/")


if __name__ == "__main__":
    main()
