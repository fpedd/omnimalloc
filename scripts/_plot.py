#
# SPDX-License-Identifier: Apache-2.0
#
"""House style and helpers shared by the benchmark and stress scripts."""

from math import isnan, nan
from statistics import mean
from typing import TYPE_CHECKING, Any

import matplotlib.pyplot as plt

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
# Most-separated hues first; per-series markers keep identity readable
SERIES = ("#2a78d6", "#1baf7a", "#eda100", "#e34948", "#4a3aa7", "#008300", "#e87ba4")
MARKERS = ("o", "s", "^", "D", "v", "P", "X")

Sample = dict[str, Any]


def fmt_seconds(seconds: float) -> str:
    if isnan(seconds):
        return "-"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.0f} us"
    if seconds < 1.0:
        return f"{seconds * 1e3:.1f} ms"
    return f"{seconds:.2f} s"


def series_means(
    samples: list[Sample],
    sizes: list[int],
    family: str,
    name: str,
    key: str,
    expected: int,
) -> list[float]:
    """Mean of `key[name]` per size; NaN where fewer than `expected` samples."""
    means = []
    for size in sizes:
        values = [
            s[key][name]
            for s in samples
            if s["family"] == family and s["size"] == size and name in s.get(key, {})
        ]
        means.append(mean(values) if len(values) >= expected else nan)
    return means


def two_panel_figure() -> "tuple[Figure, Axes, Axes]":
    """A tall time panel over a short quality panel sharing the x axis."""
    fig, (top, bottom) = plt.subplots(
        2,
        1,
        sharex=True,
        figsize=(8, 6.4),
        height_ratios=(3, 1.4),
        layout="constrained",
    )
    fig.set_facecolor(SURFACE)
    return fig, top, bottom


def style_axes(ax: "Axes") -> None:
    ax.set_facecolor(SURFACE)
    ax.grid(visible=True, which="major", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(colors=INK_MUTED, labelcolor=INK_SECONDARY, labelsize=9)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("bottom", "left"):
        ax.spines[side].set_color(AXIS)


def log_time_axes(ax: "Axes", xs: list[int], base: int = 10) -> None:
    style_axes(ax)
    ax.set_yscale("log")
    ax.set_xscale("log", base=base)
    ax.set_xticks(xs, [f"{x:,}" for x in xs])
    ax.tick_params(which="minor", bottom=False)
    ax.set_ylabel("wall time [s]", color=INK_SECONDARY, fontsize=10)


def plot_series(
    ax: "Axes",
    xs: list[int],
    values: list[float],
    label: str,
    color: str,
    marker: str = "o",
    linestyle: str = "-",
) -> None:
    # An all-NaN series would leave a log axis without positive values and
    # crash on save
    if all(isnan(value) for value in values):
        return
    ax.plot(
        xs,
        values,
        label=label,
        color=color,
        linestyle=linestyle,
        linewidth=2,
        marker=marker,
        markersize=5.5,
        markeredgecolor=SURFACE,
        markeredgewidth=1,
    )


def titles(ax: "Axes", title: str, caption: str) -> None:
    ax.set_title(title, loc="left", color=INK, fontsize=12, fontweight="medium", pad=22)
    note = ax.text(
        0.0, 1.04, caption, transform=ax.transAxes, fontsize=8.5, color=INK_MUTED
    )
    note.set_in_layout(False)
