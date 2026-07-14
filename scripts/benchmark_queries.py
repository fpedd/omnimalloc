#
# SPDX-License-Identifier: Apache-2.0
#
"""Benchmark and fuzz the pressure/conflict queries in ``primitives.queries``.

Sweeps problem size over randomized ``SyncPatternSource`` workloads (every
sync pattern x several seeds) and renders one figure per API function:

- ``get_conflicts``: wall time, one series per sync pattern
- ``get_pressure``: wall time per sync pattern, plus how far the max-weight
  antichain (A) overshoots the exact realizable peak (B)
- ``get_per_allocation_pressure``: wall time per ``Guarantee``, the mean
  overestimation versus exact peaks, and the share of instances the default
  EXACT configuration resolves

Guarantees are requested explicitly (the API defaults to BOUND): ANTICHAIN
runs with ``closure_cap=0`` to isolate the pinned-flow path (otherwise it
would ride the same join closure as EXACT), and ``get_pressure`` is timed at
ANTICHAIN so the pressure figure keeps measuring the max-weight antichain
(A). Any query whose single call exceeds ``--budget`` seconds is dropped for
the rest of the sweep; partially sampled points are not plotted, so every
plotted point averages the full workload mix.

Every instance also runs invariant cross-checks: conflict symmetry, guarantee
monotonicity, peaks bounded below by sizes and above by the global antichain,
and a brute-force oracle on tiny instances. Any violation aborts the run, so
the sweep doubles as a fuzz/torture pass over the query implementations.

    uv run python scripts/benchmark_queries.py --out benchmark_results_queries
"""

from __future__ import annotations

import argparse
from functools import partial
from math import isnan, nan
from pathlib import Path
from statistics import mean
from typing import TYPE_CHECKING, Any, TypeVar

import matplotlib.pyplot as plt
from omnimalloc.benchmark.sources.sync_patterns import SYNC_PATTERNS, SyncPatternSource
from omnimalloc.benchmark.timer import Timer
from omnimalloc.primitives.queries import (
    Guarantee,
    get_conflicts,
    get_per_allocation_pressure,
    get_pressure,
)
from omnimalloc.primitives.vector_clock import happens_before, time_components

if TYPE_CHECKING:
    from collections.abc import Callable

    from matplotlib.axes import Axes
    from matplotlib.figure import Figure
    from omnimalloc.primitives import Allocation, IdType

SIZES = (8, 16, 32, 64, 128, 256)
BRUTE_FORCE_LIMIT = 14

GUARANTEES = (Guarantee.BOUND, Guarantee.ANTICHAIN, Guarantee.EXACT)
GUARANTEE_LABELS = {
    Guarantee.BOUND: "BOUND (greedy placement)",
    Guarantee.ANTICHAIN: "ANTICHAIN (pinned flows)",
    Guarantee.EXACT: "EXACT (join closure)",
}

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SERIES = ("#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7", "#e34948", "#e87ba4")
GUARANTEE_COLORS = dict(zip(GUARANTEES, SERIES, strict=False))

Peaks = dict["IdType", int]
Sample = dict[str, Any]
T = TypeVar("T")


def _timed(call: Callable[[], T]) -> tuple[float, T | None]:
    """Wall time of one call; unresolved-EXACT raises count but yield None."""
    timer = Timer(auto_start=True)
    try:
        result = call()
    except RuntimeError as error:
        if "unresolved" not in str(error):
            raise
        result = None
    timer.stop()
    seconds = timer.elapsed_s
    if result is not None and seconds < 1e-3:
        for _ in range(4):
            timer = Timer(auto_start=True)
            call()
            timer.stop()
            seconds = min(seconds, timer.elapsed_s)
    return seconds, result


def _brute_peaks(allocations: tuple[Allocation, ...]) -> Peaks:
    """Exact per-allocation peaks by enumerating all candidate resident sets."""
    births = [time_components(alloc.start) for alloc in allocations]
    deaths = [time_components(alloc.end) for alloc in allocations]
    peaks = {alloc.id: alloc.size for alloc in allocations}
    for mask in range(1, 1 << len(allocations)):
        members = [i for i in range(len(allocations)) if mask >> i & 1]
        cut = tuple(max(c) for c in zip(*(births[i] for i in members), strict=True))
        resident = all(
            happens_before(births[i], cut) and not happens_before(deaths[i], cut)
            for i in members
        )
        if resident:
            weight = sum(allocations[i].size for i in members)
            for i in members:
                peaks[allocations[i].id] = max(peaks[allocations[i].id], weight)
    return peaks


def _check_conflicts(
    conflicts: dict[IdType, set[IdType]] | None, ids: list[IdType], context: str
) -> None:
    if conflicts is None:
        return
    assert set(conflicts) == set(ids), context
    for alloc_id, others in conflicts.items():
        assert alloc_id not in others, context
        assert all(alloc_id in conflicts[other] for other in others), context


def _check_exact_ref(
    exact_ref: Peaks | None,
    peaks: dict[Guarantee, Peaks | None],
    pressure: int | None,
    ids: list[IdType],
    context: str,
) -> None:
    if exact_ref is None:
        return
    if pressure is not None:
        assert max(exact_ref.values()) <= pressure, context
    for looser in (Guarantee.ANTICHAIN, Guarantee.BOUND):
        values = peaks.get(looser)
        if values is not None:
            assert all(exact_ref[i] <= values[i] for i in ids), context
    exact = peaks.get(Guarantee.EXACT)
    if exact is not None:
        assert exact == exact_ref, context


def _check_invariants(
    allocations: tuple[Allocation, ...],
    conflicts: dict[IdType, set[IdType]] | None,
    pressure: int | None,
    peaks: dict[Guarantee, Peaks | None],
    exact_ref: Peaks | None,
    context: str,
) -> None:
    ids = [alloc.id for alloc in allocations]
    sizes = {alloc.id: alloc.size for alloc in allocations}
    _check_conflicts(conflicts, ids, context)
    for values in peaks.values():
        if values is None:
            continue
        assert set(values) == set(ids), context
        assert all(values[i] >= sizes[i] for i in ids), context
    antichain = peaks.get(Guarantee.ANTICHAIN)
    if antichain is not None:
        if pressure is not None:
            assert max(antichain.values()) <= pressure, context
        bound = peaks.get(Guarantee.BOUND)
        if bound is not None:
            assert all(antichain[i] <= bound[i] for i in ids), context
    _check_exact_ref(exact_ref, peaks, pressure, ids, context)


def _run_queries(
    allocations: tuple[Allocation, ...],
    sample: Sample,
    dropped: set[str | Guarantee],
    budget: float,
    context: str,
) -> dict[str | Guarantee, Any]:
    """Time every query not yet dropped; drop any call exceeding the budget."""
    queries: tuple[tuple[str | Guarantee, Callable[[], Any]], ...] = (
        ("conflicts", partial(get_conflicts, allocations)),
        ("pressure", partial(get_pressure, allocations, Guarantee.ANTICHAIN)),
        (
            Guarantee.BOUND,
            partial(get_per_allocation_pressure, allocations, Guarantee.BOUND),
        ),
        (
            Guarantee.ANTICHAIN,
            partial(
                get_per_allocation_pressure,
                allocations,
                Guarantee.ANTICHAIN,
                closure_cap=0,
            ),
        ),
        (
            Guarantee.EXACT,
            partial(get_per_allocation_pressure, allocations, Guarantee.EXACT),
        ),
    )
    results: dict[str | Guarantee, Any] = {}
    for key, call in queries:
        if key in dropped:
            continue
        seconds, results[key] = _timed(call)
        if isinstance(key, Guarantee):
            name = key.name
            sample["times"][key] = seconds
        else:
            name = f"get_{key}"
            sample[key] = seconds
        if seconds > budget:
            dropped.add(key)
            print(f"dropping {name} after {_fmt(seconds)} at {context}")
    return results


def _collect_sample(
    args: argparse.Namespace,
    allocations: tuple[Allocation, ...],
    dropped: set[str | Guarantee],
    size: int,
    pattern: str,
    context: str,
) -> Sample:
    sample: Sample = {"size": size, "pattern": pattern, "times": {}}
    results = _run_queries(allocations, sample, dropped, args.budget, context)
    conflicts = results.get("conflicts")
    pressure = results.get("pressure")
    assert all(
        results[key] is not None for key in ("conflicts", "pressure") if key in results
    ), context
    peaks: dict[Guarantee, Peaks | None] = {
        guarantee: results[guarantee]
        for guarantee in GUARANTEES
        if guarantee in results
    }
    if Guarantee.EXACT in peaks:
        sample["resolved"] = peaks[Guarantee.EXACT] is not None

    exact_ref = peaks.get(Guarantee.EXACT)
    if size <= BRUTE_FORCE_LIMIT:
        exact_ref = _brute_peaks(allocations)
    _check_invariants(allocations, conflicts, pressure, peaks, exact_ref, context)
    if exact_ref is not None:
        sample["ratios"] = {
            guarantee: mean(values[i] / exact_ref[i] for i in exact_ref)
            for guarantee, values in peaks.items()
            if values is not None
        }
        if pressure is not None:
            sample["pressure_ratio"] = pressure / max(exact_ref.values())
    return sample


def collect(args: argparse.Namespace) -> list[Sample]:
    samples: list[Sample] = []
    dropped: set[str | Guarantee] = set()
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
                samples.append(
                    _collect_sample(args, allocations, dropped, size, pattern, context)
                )
        print(f"size {size} done ({len(samples)} instances total)")
    return samples


def _pattern_means(
    samples: list[Sample], sizes: list[int], pattern: str, key: str, expected: int
) -> list[float]:
    means = []
    for size in sizes:
        values = [
            s[key]
            for s in samples
            if s["size"] == size and s["pattern"] == pattern and key in s
        ]
        means.append(mean(values) if len(values) >= expected else nan)
    return means


def _guarantee_means(
    samples: list[Sample], sizes: list[int], guarantee: Guarantee, expected: int
) -> list[float]:
    means = []
    for size in sizes:
        values = [
            s["times"][guarantee]
            for s in samples
            if s["size"] == size and guarantee in s["times"]
        ]
        means.append(mean(values) if len(values) >= expected else nan)
    return means


def _ratio_means(
    samples: list[Sample], sizes: list[int], guarantee: Guarantee
) -> list[float]:
    means = []
    for size in sizes:
        values = [
            s["ratios"][guarantee]
            for s in samples
            if s["size"] == size and guarantee in s.get("ratios", {})
        ]
        means.append(mean(values) if values else nan)
    return means


def _resolved_share(samples: list[Sample], sizes: list[int]) -> list[float]:
    shares = []
    for size in sizes:
        flags = [
            s["resolved"] for s in samples if s["size"] == size and "resolved" in s
        ]
        shares.append(100 * mean(flags) if flags else nan)
    return shares


def _fmt(seconds: float) -> str:
    if isnan(seconds):
        return "-"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.0f} us"
    if seconds < 1.0:
        return f"{seconds * 1e3:.1f} ms"
    return f"{seconds:.2f} s"


def _print_summary(samples: list[Sample], args: argparse.Namespace) -> None:
    expected = len(args.patterns) * args.repeats
    rows: list[tuple[str, list[str]]] = []
    for key in ("conflicts", "pressure"):
        means = []
        for size in args.sizes:
            values = [s[key] for s in samples if s["size"] == size and key in s]
            means.append(mean(values) if len(values) >= expected else nan)
        rows.append((f"get_{key}", [_fmt(value) for value in means]))
    for guarantee in GUARANTEES:
        means = _guarantee_means(samples, args.sizes, guarantee, expected)
        rows.append((guarantee.name, [_fmt(value) for value in means]))
    shares = _resolved_share(samples, args.sizes)
    rows.append(
        (
            "EXACT resolved",
            [f"{share:.0f}%" if not isnan(share) else "-" for share in shares],
        )
    )
    width = max(len(name) for name, _ in rows)
    print(f"\n{'':<{width}}  " + "".join(f"{size:>10}" for size in args.sizes))
    for name, cells in rows:
        print(f"{name:<{width}}  " + "".join(f"{cell:>10}" for cell in cells))
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


def _direct_labels(
    ax: Axes, sizes: list[int], series: list[tuple[list[float], str]]
) -> None:
    """Label each line at its last point, nudging log-scale collisions apart."""
    ends = []
    for values, text in series:
        finite = [(x, y) for x, y in zip(sizes, values, strict=True) if not isnan(y)]
        if finite:
            ends.append((*finite[-1], text))
    ends.sort(key=lambda end: end[1])
    offsets = [0.0] * len(ends)
    for i in range(1, len(ends)):
        if ends[i][1] / ends[i - 1][1] < 1.4:
            offsets[i] = offsets[i - 1] + 11
    for (x, y, text), offset in zip(ends, offsets, strict=True):
        ax.annotate(
            text,
            (x, y),
            xytext=(7, offset),
            textcoords="offset points",
            va="center",
            fontsize=9,
            color=INK_SECONDARY,
            annotation_clip=False,
        )


def _finish_time_axes(ax: Axes, sizes: list[int]) -> None:
    _style_axes(ax)
    ax.set_yscale("log")
    ax.set_xscale("log", base=2)
    ax.set_xticks(sizes, [str(size) for size in sizes])
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


def _render_conflicts(samples: list[Sample], args: argparse.Namespace) -> Figure:
    fig, ax = plt.subplots(figsize=(8, 4.8), layout="constrained")
    fig.set_facecolor(SURFACE)
    for pattern in args.patterns:
        values = _pattern_means(samples, args.sizes, pattern, "conflicts", args.repeats)
        _plot_series(ax, args.sizes, values, pattern, _pattern_color(pattern))
    _finish_time_axes(ax, args.sizes)
    ax.set_xlabel("problem size [allocations]", color=INK_SECONDARY, fontsize=10)
    _titles(ax, "get_conflicts - wall time", _pattern_caption(args))
    _legend(ax)
    return fig


def _render_pressure(samples: list[Sample], args: argparse.Namespace) -> Figure:
    fig, (ax_time, ax_ratio) = plt.subplots(
        2,
        1,
        sharex=True,
        figsize=(8, 6.4),
        height_ratios=(3, 1.4),
        layout="constrained",
    )
    fig.set_facecolor(SURFACE)
    for pattern in args.patterns:
        color = _pattern_color(pattern)
        times = _pattern_means(samples, args.sizes, pattern, "pressure", args.repeats)
        _plot_series(ax_time, args.sizes, times, pattern, color)
        ratios = _pattern_means(samples, args.sizes, pattern, "pressure_ratio", 1)
        _plot_series(ax_ratio, args.sizes, ratios, pattern, color)
    _finish_time_axes(ax_time, args.sizes)
    _titles(
        ax_time,
        "get_pressure - wall time and antichain overshoot",
        _pattern_caption(args),
    )
    _legend(ax_time)
    _style_axes(ax_ratio)
    ax_ratio.axhline(1.0, color=AXIS, linewidth=1)
    ax_ratio.set_ylabel(
        "antichain (A) /\nexact peak (B)", color=INK_SECONDARY, fontsize=9
    )
    ax_ratio.set_xlabel("problem size [allocations]", color=INK_SECONDARY, fontsize=10)
    return fig


def _render_per_allocation(samples: list[Sample], args: argparse.Namespace) -> Figure:
    expected = len(args.patterns) * args.repeats
    fig, (ax_time, ax_ratio, ax_res) = plt.subplots(
        3,
        1,
        sharex=True,
        figsize=(8, 7.6),
        height_ratios=(3, 1.5, 1.1),
        layout="constrained",
    )
    fig.set_facecolor(SURFACE)
    labelled: list[tuple[list[float], str]] = []
    for guarantee in GUARANTEES:
        color = GUARANTEE_COLORS[guarantee]
        times = _guarantee_means(samples, args.sizes, guarantee, expected)
        _plot_series(ax_time, args.sizes, times, GUARANTEE_LABELS[guarantee], color)
        labelled.append((times, guarantee.name.lower()))
        ratios = _ratio_means(samples, args.sizes, guarantee)
        _plot_series(ax_ratio, args.sizes, ratios, GUARANTEE_LABELS[guarantee], color)
    _direct_labels(ax_time, args.sizes, labelled)
    _finish_time_axes(ax_time, args.sizes)
    _titles(
        ax_time,
        "get_per_allocation_pressure - wall time by Guarantee",
        _caption(args),
    )
    _legend(ax_time)
    _style_axes(ax_ratio)
    ax_ratio.axhline(1.0, color=AXIS, linewidth=1)
    ax_ratio.set_ylabel("mean reported /\nexact peak", color=INK_SECONDARY, fontsize=9)
    _style_axes(ax_res)
    shares = _resolved_share(samples, args.sizes)
    exact_color = GUARANTEE_COLORS[Guarantee.EXACT]
    _plot_series(ax_res, args.sizes, shares, "EXACT resolved", exact_color)
    ax_res.set_ylim(-5, 105)
    ax_res.set_ylabel("EXACT\nresolved [%]", color=INK_SECONDARY, fontsize=9)
    ax_res.set_xlabel("problem size [allocations]", color=INK_SECONDARY, fontsize=10)
    return fig


def _pattern_color(pattern: str) -> str:
    return SERIES[SYNC_PATTERNS.index(pattern)]


def _pattern_caption(args: argparse.Namespace) -> str:
    return (
        f"each point: mean over {args.repeats} seeds per sync pattern, "
        f"{args.threads} threads"
    )


def _caption(args: argparse.Namespace) -> str:
    return (
        f"each point: mean over {len(args.patterns)} sync patterns x "
        f"{args.repeats} seeds, {args.threads} threads; partial points omitted"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sizes", type=int, nargs="+", default=list(SIZES))
    parser.add_argument(
        "--patterns", nargs="+", choices=SYNC_PATTERNS, default=list(SYNC_PATTERNS)
    )
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--budget", type=float, default=10.0, help="per-call drop threshold [s]"
    )
    parser.add_argument("--out", type=Path, default=Path("benchmark_results_queries"))
    args = parser.parse_args()

    samples = collect(args)
    _print_summary(samples, args)

    args.out.mkdir(parents=True, exist_ok=True)
    figures = {
        "conflicts": _render_conflicts(samples, args),
        "pressure": _render_pressure(samples, args),
        "per_allocation_pressure": _render_per_allocation(samples, args),
    }
    for name, figure in figures.items():
        figure.savefig(args.out / f"{name}.pdf")
        plt.close(figure)
    print(f"wrote figures to {args.out}/")


if __name__ == "__main__":
    main()
