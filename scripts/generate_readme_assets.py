#
# SPDX-License-Identifier: Apache-2.0
#
"""Generate the figures embedded in the README.

Runs a deterministic benchmark suite and renders four figures, each in a light
and a dark variant with transparent backgrounds (GitHub picks the right one via
a ``<picture>`` element):

- ``hero``: packing efficiency vs. solve time across allocators (Pareto view)
- ``quality``: per-problem packing efficiency for three allocators
- ``scaling``: solve time vs. problem size
- ``allocation``: a solved allocation rendered as offset/time rectangles

Regenerate the committed assets with:

    uv run python scripts/generate_readme_assets.py

The benchmark portion takes a few minutes (every search allocator runs at the
library-wide 3 s budget). Use ``--dump data.json`` once and ``--data data.json``
to iterate on rendering without re-running it. ``--preview DIR`` additionally
writes PNG previews.
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import TYPE_CHECKING, Any

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Rectangle
from matplotlib.ticker import FuncFormatter, MultipleLocator
from omnimalloc import run_allocation, validate_allocation
from omnimalloc.allocators import BaseAllocator
from omnimalloc.benchmark.sources import BaseSource
from omnimalloc.benchmark.timer import Timer
from omnimalloc.common.constants import DEFAULT_TIMEOUT, MB

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure
    from omnimalloc.primitives import Pool

SEED = 0
MINIMALLOC_URL = "git+https://github.com/google/minimalloc.git"
SCALING_SIZES = (10, 32, 100, 316, 1000, 3162, 10000)
SCALING_SIZES_SLOW = SCALING_SIZES[:-1]  # minimalloc cannot solve 10k in budget
ALLOCATION_PROBLEM = "mm-G"

ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"

# Allocator display metadata: registry name -> (label, palette role).
ALLOCATORS: dict[str, tuple[str, str]] = {
    "naive_allocator": ("naive", "baseline"),
    "random_allocator": ("random search", "baseline"),
    "greedy_by_size_allocator_cpp": ("greedy (size)", "greedy"),
    "greedy_by_all_allocator_cpp": ("greedy (all)", "greedy"),
    "best_fit_allocator": ("best-fit", "greedy_alt"),
    "omni_allocator": ("omni", "omni"),
    "hill_climb_allocator": ("hill climbing", "search_alt"),
    "genetic_allocator": ("genetic", "search_alt"),
    "simulated_annealing_allocator": ("simulated annealing", "search"),
    "tabu_search_allocator": ("tabu search", "search"),
    "telamalloc_allocator": ("telamalloc", "telamalloc"),
    "minimalloc_allocator": ("minimalloc", "minimalloc"),
    "supermalloc_allocator": ("supermalloc", "exact"),
}

HERO_ALLOCATORS = (
    "random_allocator",
    "greedy_by_size_allocator_cpp",
    "greedy_by_all_allocator_cpp",
    "best_fit_allocator",
    "omni_allocator",
    "hill_climb_allocator",
    "genetic_allocator",
    "simulated_annealing_allocator",
    "tabu_search_allocator",
    "telamalloc_allocator",
    "minimalloc_allocator",
    "supermalloc_allocator",
)
QUALITY_ALLOCATORS = (
    "greedy_by_size_allocator_cpp",
    "best_fit_allocator",
    "omni_allocator",
    "tabu_search_allocator",
    "telamalloc_allocator",
    "minimalloc_allocator",
    "supermalloc_allocator",
)
# best-fit is omitted: its curve coincides with greedy (size) at every size.
SCALING_ALLOCATORS = (
    "naive_allocator",
    "greedy_by_size_allocator_cpp",
    "omni_allocator",
    "hill_climb_allocator",
    "telamalloc_allocator",
    "minimalloc_allocator",
    "supermalloc_allocator",
)

QUALITY_PROBLEMS = ("mm-A", "mm-C", "mm-H", "mm-K", "pinwheel", "tiling", "random")
PROBLEM_LABELS = {
    "mm-A": "minimalloc A",
    "mm-C": "minimalloc C",
    "mm-G": "minimalloc G",
    "mm-H": "minimalloc H",
    "mm-K": "minimalloc K",
    "pinwheel": "pinwheel",
    "tiling": "tiling",
    "random": "random (easy)",
}

# Direct-label offsets in points, tuned per hero point: (dx, dy, ha).
HERO_LABEL_OFFSETS: dict[str, tuple[float, float, str]] = {
    "random_allocator": (0, -11, "center"),
    "greedy_by_size_allocator_cpp": (8, 0, "left"),
    "greedy_by_all_allocator_cpp": (0, -11, "center"),
    "best_fit_allocator": (8, 0, "left"),
    "omni_allocator": (8, 0, "left"),
    "hill_climb_allocator": (0, -11, "center"),
    "genetic_allocator": (8, -6, "left"),
    "simulated_annealing_allocator": (-8, 3, "right"),
    "tabu_search_allocator": (-8, 0, "right"),
    "telamalloc_allocator": (0, -11, "center"),
    "minimalloc_allocator": (8, 0, "left"),
    "supermalloc_allocator": (-10, 0, "right"),
}

# Direct-label offsets in points: (dx, dy, ha). minimalloc's line ends early
# (no 10k point), so its label anchors left, away from the 10k label cluster;
# the four lines that converge on the 3 s budget at 10k get staggered dy.
SCALING_LABEL_OFFSETS = {
    "naive_allocator": (4, -2, "left"),
    "greedy_by_size_allocator_cpp": (4, -4, "left"),
    "omni_allocator": (4, 4, "left"),
    "hill_climb_allocator": (4, 3, "left"),
    "telamalloc_allocator": (4, -11, "left"),
    "minimalloc_allocator": (0, 9, "center"),
    "supermalloc_allocator": (4, 10, "left"),
}


@dataclass(frozen=True)
class Theme:
    name: str
    ink: str
    muted: str
    faint: str
    grid: str
    optimal: str
    role: dict[str, str]  # baseline / greedy / search / exact


LIGHT = Theme(
    name="light",
    ink="#24292f",
    muted="#59626d",
    faint="#818b98",
    grid="#d0d7de",
    optimal="#1a7f37",
    role={
        "baseline": "#848d97",
        "greedy": "#0969da",
        "greedy_alt": "#54aeff",
        "omni": "#cf222e",
        "search": "#bc4c00",
        "search_alt": "#9a6700",
        "telamalloc": "#1b7c83",
        "minimalloc": "#bf3989",
        "exact": "#8250df",
    },
)
DARK = Theme(
    name="dark",
    ink="#e6edf3",
    muted="#9198a1",
    faint="#767e88",
    grid="#30363d",
    optimal="#3fb950",
    role={
        "baseline": "#768390",
        "greedy": "#4493f8",
        "greedy_alt": "#79c0ff",
        "omni": "#f85149",
        "search": "#f0883e",
        "search_alt": "#d29922",
        "telamalloc": "#39c5cf",
        "minimalloc": "#db61a2",
        "exact": "#ab7df8",
    },
)


def _pip_install(spec: str) -> None:
    """Install a package into the running interpreter, preferring uv over pip."""
    for cmd in (
        ["uv", "pip", "install", "--python", sys.executable, spec],
        [sys.executable, "-m", "pip", "install", spec],
    ):
        if shutil.which(cmd[0]) and subprocess.run(cmd, check=False).returncode == 0:
            return
    raise RuntimeError(f"Could not install {spec!r}; install it manually")


def _ensure_minimalloc() -> None:
    """Install Google's minimalloc on demand (no PyPI wheel)."""
    try:
        import minimalloc  # type: ignore  # noqa: F401
    except ImportError:
        print(f"minimalloc not installed — installing from {MINIMALLOC_URL} ...")
        _pip_install(MINIMALLOC_URL)


def _solve(
    pool: Pool, allocator: BaseAllocator, *, validate: bool = True
) -> tuple[float, float, Pool]:
    """Time the solve alone; validation is quadratic and would skew timings."""
    with Timer() as timer:
        solved = run_allocation(pool, allocator=allocator)
    if validate:
        validate_allocation(solved)
    return timer.elapsed_s, solved.efficiency, solved


def _hard_suite() -> dict[str, Pool]:
    """Real minimalloc benchmarks plus adversarial synthetic patterns."""
    suite: dict[str, Pool] = {}
    minimalloc = BaseSource.get("minimalloc_source")()
    for variant in minimalloc.get_available_variants():
        suite[f"mm-{variant.split('.')[0]}"] = minimalloc.get_variant(variant)
    suite["pinwheel"] = BaseSource.get("pinwheel_source")().get_variant(101)
    suite["tiling"] = BaseSource.get("tiling_source")().get_variant(100)
    suite["random"] = BaseSource.get("random_source")().get_variant(250)
    return suite


def collect_data() -> dict[str, Any]:
    _ensure_minimalloc()
    random.seed(SEED)
    suite = _hard_suite()
    hard = [k for k in suite if k != "random"]

    # Hero + quality: every problem, but "random" only where quality needs it.
    runs: dict[str, dict[str, tuple[float, float]]] = {}
    supermalloc_pools: dict[str, Pool] = {}
    names = set(HERO_ALLOCATORS) | set(QUALITY_ALLOCATORS)
    for name in sorted(names):
        allocator = BaseAllocator.resolve(name)
        problems = tuple(suite) if name in QUALITY_ALLOCATORS else hard
        runs[name] = {}
        for problem in problems:
            seconds, efficiency, solved = _solve(suite[problem], allocator)
            if name == "supermalloc_allocator":
                supermalloc_pools[problem] = solved
            runs[name][problem] = (seconds, efficiency)
            print(f"{name:38s} {problem:10s} {seconds:8.3f}s  {efficiency:7.2%}")

    hero = {
        name: {
            "seconds": mean(runs[name][p][0] for p in hard),
            "efficiency": mean(runs[name][p][1] for p in hard),
        }
        for name in HERO_ALLOCATORS
    }
    quality = {
        name: {p: runs[name][p][1] for p in QUALITY_PROBLEMS}
        for name in QUALITY_ALLOCATORS
    }

    # Scaling: solve time vs. problem size on the random source. Skip validation
    # here: it is quadratic in pure Python and would dwarf the fast solves at 10k.
    source = BaseSource.get("random_source")()
    scaling: dict[str, list[list[float]]] = {}
    for name in SCALING_ALLOCATORS:
        allocator = BaseAllocator.resolve(name)
        # minimalloc can't solve 10k within the budget and would error out.
        capped = name == "minimalloc_allocator"
        sizes = SCALING_SIZES_SLOW if capped else SCALING_SIZES
        scaling[name] = []
        for size in sizes:
            seconds, _, _ = _solve(source.get_variant(size), allocator, validate=False)
            scaling[name].append([size, seconds])
            print(f"{name:38s} n={size:<6d} {seconds:8.3f}s")

    # Allocation rendering: a real problem solved to proven optimality. The loop
    # above already solved it with the same budget, so reuse that pool.
    solved = supermalloc_pools[ALLOCATION_PROBLEM]
    allocation = {
        "problem": PROBLEM_LABELS.get(ALLOCATION_PROBLEM, ALLOCATION_PROBLEM),
        "efficiency": runs["supermalloc_allocator"][ALLOCATION_PROBLEM][1],
        "size": solved.size,
        "rects": [[a.start, a.duration, a.offset, a.size] for a in solved.allocations],
    }

    return {
        "hero": hero,
        "quality": quality,
        "scaling": scaling,
        "allocation": allocation,
    }


def _style(theme: Theme) -> dict[str, Any]:
    return {
        "font.family": ["Lato", "DejaVu Sans"],
        "font.size": 9.0,
        "svg.fonttype": "path",
        "figure.facecolor": "none",
        "axes.facecolor": "none",
        "savefig.facecolor": "none",
        "savefig.transparent": True,
        "axes.spines.left": False,
        "axes.spines.right": False,
        "axes.spines.top": False,
        "axes.spines.bottom": False,
        "axes.labelcolor": theme.muted,
        "axes.labelsize": 8.5,
        "grid.color": theme.grid,
        "grid.linewidth": 0.5,
        "grid.alpha": 0.45,
        "xtick.labelcolor": theme.muted,
        "ytick.labelcolor": theme.muted,
        "xtick.labelsize": 8.0,
        "ytick.labelsize": 8.0,
        "xtick.major.size": 0,
        "ytick.major.size": 0,
        "xtick.minor.size": 0,
        "ytick.minor.size": 0,
    }


def _title(fig: Figure, theme: Theme, title: str, subtitle: str) -> None:
    fig.text(
        0.01,
        0.99,
        title,
        ha="left",
        va="top",
        fontsize=11.5,
        fontweight="bold",
        color=theme.ink,
    )
    fig.text(
        0.01, 0.905, subtitle, ha="left", va="top", fontsize=8.5, color=theme.muted
    )


def _series_line(fig: Figure, series: list[tuple[str, str]], y: float) -> None:
    """Draw an inline legend: colored '● label' entries on one figure line."""
    x = 0.012
    for label, color in series:
        fig.text(x, y, f"● {label}", ha="left", va="top", fontsize=8, color=color)
        x += 0.033 + len(label) * 0.0148


def _optimal_line(
    ax: Axes,
    theme: Theme,
    value: float,
    axis: str = "y",
    linewidth: float = 0.8,
    alpha: float = 0.8,
    zorder: int = 1,
) -> None:
    """Dashed reference line marking the proven-optimal value."""
    line = ax.axhline if axis == "y" else ax.axvline
    line(
        value,
        color=theme.optimal,
        linewidth=linewidth,
        linestyle=(0, (4, 4)),
        alpha=alpha,
        zorder=zorder,
    )


def _format_seconds(value: float) -> str:
    if value < 1e-3:
        return f"{value * 1e6:.0f} µs"
    if value < 1:
        return f"{value * 1e3:.0f} ms"
    return f"{value:.0f} s"


def _format_steps(value: float, _pos: int) -> str:
    if value >= 1e6:
        return f"{value / 1e6:g}M"
    if value >= 1e3:
        return f"{value / 1e3:g}k"
    return f"{value:g}"


def _save(fig: Figure, name: str, theme: Theme, preview: Path | None) -> None:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        ASSETS_DIR / f"{name}_{theme.name}.svg", bbox_inches="tight", pad_inches=0.02
    )
    if preview is not None:
        preview.mkdir(parents=True, exist_ok=True)
        fig.savefig(
            preview / f"{name}_{theme.name}.png",
            dpi=220,
            bbox_inches="tight",
            pad_inches=0.02,
            facecolor="#ffffff" if theme.name == "light" else "#0d1117",
            transparent=False,
        )
    plt.close(fig)


def render_hero(data: dict[str, Any], theme: Theme, preview: Path | None) -> None:
    fig, ax = plt.subplots(figsize=(7.4, 3.5))

    points = {
        name: (data[name]["seconds"], data[name]["efficiency"] * 100)
        for name in HERO_ALLOCATORS
    }

    _optimal_line(ax, theme, 100)
    ax.annotate(
        "optimal",
        xy=(1.0, 100),
        xycoords=("axes fraction", "data"),
        xytext=(-2, 3),
        textcoords="offset points",
        ha="right",
        fontsize=7.5,
        color=theme.optimal,
    )

    # Pareto frontier: lower time and higher efficiency dominate.
    ordered = sorted(points.values(), key=lambda p: (p[0], -p[1]))
    front, best = [], float("-inf")
    for seconds, efficiency in ordered:
        if efficiency > best:
            front.append((seconds, efficiency))
            best = efficiency
    ax.plot(
        *zip(*front, strict=False),
        color=theme.faint,
        linewidth=0.9,
        linestyle=(0, (1, 2)),
        zorder=2,
    )

    for name, (seconds, efficiency) in points.items():
        label, role = ALLOCATORS[name]
        color = theme.role[role]
        emphasis = name == "supermalloc_allocator"
        ax.scatter(
            seconds,
            efficiency,
            s=110 if emphasis else 52,
            color=color,
            linewidths=1.4 if emphasis else 0,
            edgecolors=theme.ink if emphasis else "none",
            zorder=4,
        )
        dx, dy, ha = HERO_LABEL_OFFSETS[name]
        ax.annotate(
            label,
            (seconds, efficiency),
            xytext=(dx, dy),
            textcoords="offset points",
            ha=ha,
            va="center",
            fontsize=8.5 if emphasis else 8,
            color=color,
            fontweight="bold" if emphasis else "normal",
            zorder=5,
        )

    ax.set_xscale("log")
    ax.set_xlim(0.2e-3, 22)
    ax.set_ylim(52, 104)
    ticks = (1e-3, 1e-2, 1e-1, 1, 10)
    ax.set_xticks(ticks)
    ax.set_xticklabels([_format_seconds(t) for t in ticks])
    ax.grid(visible=True, axis="both")
    ax.set_xlabel("mean solve time (log scale)")
    ax.set_ylabel("mean packing efficiency (%)")

    _title(
        fig,
        theme,
        "Solution quality vs. solve time",
        "13 hard problems: 11 real-world minimalloc benchmarks "
        "and 2 adversarial patterns · 100% = proven lower bound",
    )
    fig.subplots_adjust(top=0.80, bottom=0.13, left=0.075, right=0.985)
    _save(fig, "hero", theme, preview)


def render_quality(data: dict[str, Any], theme: Theme, preview: Path | None) -> None:
    fig, ax = plt.subplots(figsize=(3.8, 3.35))

    rows = list(QUALITY_PROBLEMS)
    ys = range(len(rows), 0, -1)

    _optimal_line(ax, theme, 100, axis="x")

    for row, y in zip(rows, ys, strict=True):
        values = [data[name][row] * 100 for name in QUALITY_ALLOCATORS]
        ax.plot(
            [min(values), max(values)],
            [y, y],
            color=theme.grid,
            linewidth=1.1,
            zorder=2,
            solid_capstyle="round",
        )
        for name, value in zip(QUALITY_ALLOCATORS, values, strict=True):
            emphasis = name == "supermalloc_allocator"
            ax.scatter(
                value,
                y,
                s=52 if emphasis else 34,
                color=theme.role[ALLOCATORS[name][1]],
                zorder=4,
                linewidths=1.2 if emphasis else 0,
                edgecolors=theme.ink if emphasis else "none",
            )

    ax.set_yticks(list(ys))
    ax.set_yticklabels([PROBLEM_LABELS[r] for r in rows], fontsize=8.5)
    ax.set_ylim(0.4, len(rows) + 0.6)
    ax.set_xlim(60, 103)
    ax.set_xticks((60, 70, 80, 90, 100))
    ax.grid(visible=True, axis="x")
    ax.set_xlabel("packing efficiency (%)")

    _title(fig, theme, "Quality per problem", "100% = proven lower bound")
    series = [
        (ALLOCATORS[name][0], theme.role[ALLOCATORS[name][1]])
        for name in QUALITY_ALLOCATORS
    ]
    _series_line(fig, series[:4], y=0.845)
    _series_line(fig, series[4:], y=0.79)
    fig.subplots_adjust(top=0.72, bottom=0.125, left=0.265, right=0.97)
    _save(fig, "quality", theme, preview)


def render_scaling(data: dict[str, Any], theme: Theme, preview: Path | None) -> None:
    fig, ax = plt.subplots(figsize=(3.8, 3.35))

    for name in SCALING_ALLOCATORS:
        label, role = ALLOCATORS[name]
        sizes, seconds = zip(*data[name], strict=True)
        color = theme.role[role]
        emphasis = name == "supermalloc_allocator"
        ax.plot(
            sizes,
            seconds,
            color=color,
            linewidth=1.8 if emphasis else 1.4,
            marker="o",
            markersize=3.4,
            markeredgewidth=0,
            zorder=4,
        )
        dx, dy, ha = SCALING_LABEL_OFFSETS[name]
        ax.annotate(
            label,
            (sizes[-1], seconds[-1]),
            xytext=(dx, dy),
            textcoords="offset points",
            ha=ha,
            va="center",
            fontsize=8,
            color=color,
            fontweight="bold" if emphasis else "normal",
        )

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlim(8, 1.5e5)
    ax.set_xticks((10, 100, 1000, 10000))
    ax.set_xticklabels(["10", "100", "1k", "10k"])
    yticks = (1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1, 10, 100)
    ax.set_ylim(3e-6, 400)
    ax.set_yticks(yticks)
    ax.set_yticklabels([_format_seconds(t) for t in yticks])
    ax.grid(visible=True, axis="both")
    ax.set_xlabel("number of allocations")
    ax.set_ylabel("solve time")

    _title(
        fig,
        theme,
        "Scaling",
        f"random problems · search budget {DEFAULT_TIMEOUT:.0f} s",
    )
    fig.subplots_adjust(top=0.845, bottom=0.125, left=0.16, right=0.97)
    _save(fig, "scaling", theme, preview)


def render_allocation(data: dict[str, Any], theme: Theme, preview: Path | None) -> None:
    fig, ax = plt.subplots(figsize=(7.4, 2.9))

    rects = data["rects"]
    size = data["size"]
    max_end = max(start + duration for start, duration, _, _ in rects)

    # Color each buffer by size quantile within the greedy-to-exact hue range.
    cmap = LinearSegmentedColormap.from_list(
        "buffers", [theme.role["greedy"], theme.role["exact"]]
    )
    ordered_sizes = sorted(r[3] for r in rects)
    for start, duration, offset, height in rects:
        quantile = ordered_sizes.index(height) / max(len(ordered_sizes) - 1, 1)
        ax.add_patch(
            Rectangle(
                (start, offset),
                duration,
                height,
                facecolor=cmap(quantile),
                alpha=0.85,
                edgecolor=theme.ink,
                linewidth=0.2,
            )
        )

    _optimal_line(ax, theme, size, linewidth=0.9, alpha=1.0, zorder=5)
    ax.annotate(
        f"peak {size / MB:.2f} MB = lower bound (proven optimal)",
        xy=(0.995, size),
        xycoords=("axes fraction", "data"),
        xytext=(0, 4),
        textcoords="offset points",
        ha="right",
        fontsize=8,
        color=theme.optimal,
        zorder=6,
    )

    ax.set_xlim(0, max_end)
    ax.set_ylim(0, size * 1.14)
    ax.yaxis.set_major_locator(MultipleLocator(0.25 * MB))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / MB:g}"))
    ax.xaxis.set_major_formatter(FuncFormatter(_format_steps))
    ax.grid(visible=True, axis="y")
    ax.set_xlabel("time step")
    ax.set_ylabel("offset (MB)")

    _title(
        fig,
        theme,
        "A solved problem",
        f"{data['problem']} · {len(rects)} buffers packed at "
        f"{data['efficiency']:.0%} efficiency by supermalloc",
    )
    fig.subplots_adjust(top=0.775, bottom=0.155, left=0.055, right=0.985)
    _save(fig, "allocation", theme, preview)


def render_all(data: dict[str, Any], preview: Path | None) -> None:
    for theme in (LIGHT, DARK):
        with mpl.rc_context(_style(theme)):
            render_hero(data["hero"], theme, preview)
            render_quality(data["quality"], theme, preview)
            render_scaling(data["scaling"], theme, preview)
            render_allocation(data["allocation"], theme, preview)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, help="load benchmark data from JSON")
    parser.add_argument("--dump", type=Path, help="write benchmark data to JSON")
    parser.add_argument("--preview", type=Path, help="also write PNG previews")
    args = parser.parse_args()

    data = json.loads(args.data.read_text()) if args.data else collect_data()
    if args.dump:
        args.dump.write_text(json.dumps(data, indent=2))

    render_all(data, args.preview)


if __name__ == "__main__":
    main()
