#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Final, Literal, NamedTuple

from omnimalloc.analysis import conflict_degrees, pressure, try_linearize
from omnimalloc.analysis.clock import time_components, uniform_dim
from omnimalloc.common.optional import require_optional
from omnimalloc.primitives import (
    Allocation,
    AllocationKind,
    IdType,
    Memory,
    Pool,
    System,
)

try:
    import matplotlib.pyplot as plt
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure
    from matplotlib.patches import Patch, Rectangle
    from matplotlib.ticker import FuncFormatter, MaxNLocator, MultipleLocator

    HAS_MATPLOTLIB = True

except ImportError:
    from types import SimpleNamespace

    HAS_MATPLOTLIB = False

    plt = SimpleNamespace(  # type: ignore[assignment]
        subplots=None,
        show=None,
    )
    Axes = None  # type: ignore[assignment,misc]
    Figure = None  # type: ignore[assignment,misc]
    Patch = None  # type: ignore[assignment,misc]
    Rectangle = None  # type: ignore[assignment,misc]
    FuncFormatter = None  # type: ignore[assignment,misc]
    MaxNLocator = None  # type: ignore[assignment,misc]
    MultipleLocator = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

_BYTE_UNITS: Final[tuple[tuple[int, str], ...]] = (
    (1024**3, "GB"),
    (1024**2, "MB"),
    (1024, "KB"),
)


def _byte_unit(value: float) -> tuple[int, str]:
    """Largest byte unit that keeps `value` at least 1 in that unit."""
    for divisor, suffix in _BYTE_UNITS:
        if value >= divisor:
            return divisor, suffix
    return 1, "B"


def _format_bytes(value: float) -> str:
    """Human-readable byte count, auto-scaled so small values don't read as 0.0 MB."""
    divisor, suffix = _byte_unit(value)
    return f"{value / divisor:.1f}{suffix}"


KIND_COLOR_MAP: Final[dict[AllocationKind, str]] = {
    AllocationKind.WORKSPACE: "C0",
    AllocationKind.CONSTANT: "C1",
    AllocationKind.INPUT: "C2",
    AllocationKind.OUTPUT: "C3",
}

LANE_CAVEAT: Final[str] = (
    "Lanes show each thread's local timeline: temporal overlaps within a lane "
    "are real conflicts, but cross-thread conflicts may not be visible "
    "anywhere; validate_allocation() is the authority."
)

PANEL_CAVEAT: Final[str] = (
    "Virtual global time is a linear extension of happens-before: visible "
    "temporal overlaps are always real conflicts, but concurrent allocations "
    "may be drawn apart; validate_allocation() is the authority."
)


def _get_allocation_color(kind: AllocationKind | None) -> str:
    if kind is None:
        kind = AllocationKind.WORKSPACE
    if kind not in KIND_COLOR_MAP:
        raise ValueError(f"Unknown allocation kind: {kind}")
    return KIND_COLOR_MAP[kind]


def _memory_dim(memory: Memory) -> int:
    # Dimension is uniform per pool (the validate.py contract), but pools of
    # one memory may mix, e.g. after linearizing one pool; lanes cover the max.
    return max((uniform_dim(pool.allocations) for pool in memory.pools), default=1)


def _lane_extent(alloc: Allocation, lane: int) -> tuple[int, int]:
    """Project an allocation's lifetime onto one thread's local timeline."""
    return time_components(alloc.start)[lane], time_components(alloc.end)[lane]


def _sum_extent(alloc: Allocation) -> tuple[int, int]:
    """Project an allocation's lifetime onto the monotone clock-component sum."""
    return sum(time_components(alloc.start)), sum(time_components(alloc.end))


class _Panel(NamedTuple):
    """One subplot: a memory drawn over one projection of its lifetimes."""

    memory: Memory
    extents: dict[int, tuple[int, int]]  # id(allocation) -> projected lifetime
    xlabel: str
    title: str | None = None
    note: str | None = None

    @property
    def x_limits(self) -> tuple[int, int]:
        """X-limits covering the panel's extents; empty panels keep a (0, 1) axis."""
        return 0, max((end for _, end in self.extents.values()), default=1)


def _memory_allocations(memory: Memory) -> list[Allocation]:
    return [alloc for pool in memory.pools for alloc in pool.allocations]


def _projected(alloc: Allocation, extent: tuple[int, int]) -> Allocation:
    """Scalar stand-in carrying the allocation's size over a projected extent."""
    return Allocation(id=alloc.id, size=alloc.size, start=extent[0], end=extent[1])


def _panel_extents(memory: Memory) -> tuple[dict[int, tuple[int, int]], bool]:
    """Projected lifetimes for one memory's panel; True when conflict-exact."""
    allocations = _memory_allocations(memory)
    if len({alloc.dim for alloc in allocations}) == 1:
        linearized = try_linearize(tuple(allocations))
        if linearized is not None:
            return {
                id(alloc): (lin.start, lin.end)
                for alloc, lin in zip(allocations, linearized, strict=True)
            }, True
    return {id(alloc): _sum_extent(alloc) for alloc in allocations}, False


def _conflict_pairs(allocations: tuple[Allocation, ...]) -> int | None:
    """Count conflicting allocation pairs, or None once over budget."""
    try:
        degrees = conflict_degrees(allocations)
    except RuntimeError:
        return None
    return sum(degrees) // 2


def _conflict_visibility(
    memory: Memory, extents: dict[int, tuple[int, int]]
) -> tuple[int, int] | None:
    """Same-pool conflict pairs (visible under the projection, total), or None.

    Counting conflict pairs is quadratic in the worst case; the default work
    budget gives up (None) rather than stall rendering on huge memories.
    """
    visible = total = 0
    for pool in memory.pools:
        projected = tuple(_projected(a, extents[id(a)]) for a in pool.allocations)
        pool_visible = _conflict_pairs(projected)
        pool_total = _conflict_pairs(pool.allocations)
        if pool_visible is None or pool_total is None:
            return None
        visible += pool_visible
        total += pool_total
    return visible, total


def _visible_lane_extents(
    allocations: list[Allocation], lane: int
) -> list[tuple[Allocation, tuple[int, int]]]:
    """Allocations visible in one lane, with their local-time extents."""
    visible = []
    for alloc in allocations:
        if lane >= alloc.dim:
            continue  # Lower-dim allocation in a mixed memory; no such lane
        start, end = _lane_extent(alloc, lane)
        if start == end:
            continue  # No local time on this thread; not visible in this lane
        visible.append((alloc, (start, end)))
    return visible


def _lane_peaks(allocations: list[Allocation], dim: int) -> list[int]:
    """Definite-occupancy peak per lane: pressure of its projected extents."""
    peaks = []
    for lane in range(dim):
        projected = tuple(
            _projected(alloc, extent)
            for alloc, extent in _visible_lane_extents(allocations, lane)
        )
        peaks.append(pressure(projected))
    return peaks


def _select_lanes(
    allocations: list[Allocation], dim: int, max_lanes: int | None
) -> list[int]:
    """Lanes to draw: all, or the top-k by definite-occupancy peak."""
    if max_lanes is None or dim <= max_lanes:
        return list(range(dim))
    peaks = _lane_peaks(allocations, dim)
    ranked = sorted(range(dim), key=lambda lane: (-peaks[lane], lane))
    return sorted(ranked[:max_lanes])


def _get_y_limits(system: System) -> dict[Memory, tuple[int, int]]:
    limits: dict[Memory, tuple[int, int]] = {}
    for memory in system.memories:
        size = memory.size
        used = memory.used_size

        if size is None:
            # No size declared, scale to 1.2x used
            y_limit = used * 1.2

        elif used > size:
            # Usage exceeds the declared size, scale to 1.2x usage
            y_limit = used * 1.2

        elif used >= size * 0.5:
            # Usage is 50-100% of the declared size, use the size as limit
            y_limit = size

        else:
            # Usage below 50% of the declared size, scale to 2x usage
            y_limit = used * 2

        # Clamp to at least 1 so downstream tick spacing stays positive
        # even for entities with zero used memory.
        limits[memory] = (0, max(int(y_limit), 1))

    return limits


def _get_y_offsets(system: System) -> dict[Memory, dict[Pool, int]]:
    offsets: dict[Memory, dict[Pool, int]] = defaultdict(dict)
    for memory in system.memories:
        current_offset = 0
        for pool in memory.pools:
            if pool.offset is not None:
                offsets[memory][pool] = pool.offset
            else:
                offsets[memory][pool] = current_offset
                current_offset += pool.size

    return offsets


def _draw_allocation(
    ax: Axes,
    alloc: Allocation,
    offset: int,
    color: str,
    extent: tuple[int, int],
) -> None:
    """Draw a single allocation rectangle over its projected lifetime."""
    assert alloc.offset is not None
    start, end = extent
    y_pos = offset + alloc.offset
    rect = Rectangle(
        xy=(start, y_pos),
        width=end - start,
        height=alloc.size,
        edgecolor="black",
        facecolor=color,
        alpha=0.5,
    )
    ax.add_patch(rect)
    ax.text(
        (start + end) / 2,
        y_pos + alloc.size / 2,
        f"{alloc.id}",
        ha="center",
        va="center",
        fontsize=8,
    )


def _draw_pool_background(
    ax: Axes, y_offset: int, pool_size: int, colors: set[str]
) -> None:
    """Draw background rectangle for allocation pool (gray for mixed/empty kinds)."""
    color = next(iter(colors)) if len(colors) == 1 else "gray"
    x_min, x_max = ax.get_xlim()
    rect = Rectangle(
        xy=(x_min, y_offset),
        width=x_max - x_min,
        height=pool_size,
        edgecolor=color,
        facecolor=color,
        alpha=0.2,
    )
    ax.add_patch(rect)


def _draw_limit_lines(ax: Axes, limits: dict[str, int]) -> None:
    """Draw annotated horizontal lines for used size, declared size, and extras."""
    _, x_max = ax.get_xlim()
    for name, value in limits.items():
        ax.axhline(value, color="black", linestyle="--", linewidth=1, alpha=0.8)
        ax.annotate(
            f"{_format_bytes(value)}\n{name}",
            xy=(x_max, value),
            xytext=(x_max * 1.02, value * 1.01),
            ha="left",
            va="center",
            fontsize=9,
            color="black",
            alpha=0.8,
            bbox={
                "boxstyle": "round,pad=0.3",
                "fc": "white",
                "ec": "gray",
                "alpha": 0.8,
            },
            arrowprops={"arrowstyle": "->", "color": "gray", "lw": 0.8},
        )


def _set_axes_ticks(ax: Axes, y_limit: int, num_ticks: int = 8) -> None:
    """Configure axis ticks and formatters."""
    tick_size = y_limit / num_ticks
    divisor, unit = _byte_unit(y_limit)
    ax.yaxis.set_major_locator(MultipleLocator(tick_size))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x / divisor:.1f}"))
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(visible=True, alpha=0.5)
    ax.set_ylabel(f"Memory ({unit})")


def _memory_title(memory: Memory, threads: str) -> str:
    size = memory.size
    size_str = _format_bytes(size) if size is not None else "Unbounded Size"
    return f"{memory.id} ({size_str}, {len(memory.pools)} pools{threads})"


def _lane_panels(
    system: System, max_lanes: int | None
) -> tuple[list[_Panel], str | None]:
    """One panel per (memory, thread): each thread's local-time projection."""
    dims = {memory: _memory_dim(memory) for memory in system.memories}
    panels = []
    for memory in system.memories:
        dim = dims[memory]
        allocations = _memory_allocations(memory)
        lanes = _select_lanes(allocations, dim, max_lanes)
        threads = f", {dim} threads" if dim > 1 else ""
        if len(lanes) < dim:
            threads = f", top {len(lanes)} of {dim} threads"
        for index, lane in enumerate(lanes):
            extents = {
                id(alloc): extent
                for alloc, extent in _visible_lane_extents(allocations, lane)
            }
            panels.append(
                _Panel(
                    memory=memory,
                    extents=extents,
                    xlabel="Time (Step)" if dim == 1 else f"Thread {lane} Time (Step)",
                    title=_memory_title(memory, threads) if index == 0 else None,
                )
            )
    caveat = LANE_CAVEAT if any(dim > 1 for dim in dims.values()) else None
    return panels, caveat


def _projection_panels(system: System) -> tuple[list[_Panel], str | None]:
    """One panel per memory over a happens-before-monotone virtual time."""
    panels = []
    caveat = None
    for memory in system.memories:
        dim = _memory_dim(memory)
        extents, exact = _panel_extents(memory)
        if dim == 1:
            threads, xlabel, note = "", "Time (Step)", None
        elif exact:
            threads = f", {dim} threads, linearized"
            xlabel, note = "Virtual Time (Step)", None
        else:
            threads, xlabel = f", {dim} threads", "Virtual Global Time (Step)"
            note = None
            visibility = _conflict_visibility(memory, extents)
            if visibility is not None:
                visible, total = visibility
                note = f"{visible}/{total} conflicts visible" if total else None
            caveat = PANEL_CAVEAT
        panels.append(
            _Panel(
                memory=memory,
                extents=extents,
                xlabel=xlabel,
                title=_memory_title(memory, threads),
                note=note,
            )
        )
    return panels, caveat


def _add_legend(fig: Figure) -> None:
    """Add figure legend for allocation kinds."""
    handles = [
        Patch(color=color, label=kind.name, alpha=0.8)
        for kind, color in KIND_COLOR_MAP.items()
    ]
    fig.legend(
        handles=handles,
        loc="outside lower center",
        ncol=len(handles),
        fontsize=8,
        title="Allocation Kinds",
    )


def _set_axes_limits(
    ax: Axes,
    x_limits: tuple[int, int],
    y_limits: tuple[int, int],
    size: int | None,
) -> None:
    """Set axis limits and add scaling notice if needed."""
    ax.set_xlim(x_limits)
    ax.set_ylim(y_limits)
    if size is not None and y_limits[1] < size:
        ax.text(
            0.02,
            0.98,
            "Y-axis scaled down for improved readability",
            transform=ax.transAxes,
            va="top",
            fontsize=8,
            alpha=0.7,
        )


# TODO(fpedd): Add a pools size descriptor on the right side of each pool


def _draw_panel(
    ax: Axes,
    panel: _Panel,
    y_limits: tuple[int, int],
    y_offsets: dict[Pool, int],
    capacities: dict[str, dict[IdType, int]],
) -> None:
    memory = panel.memory
    if panel.title is not None:
        ax.set_title(panel.title)
    ax.set_xlabel(panel.xlabel)
    _set_axes_limits(ax, panel.x_limits, y_limits, memory.size)
    _set_axes_ticks(ax, y_limits[1])
    if panel.note is not None:
        ax.text(
            0.98,
            0.98,
            panel.note,
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=8,
            alpha=0.7,
        )

    for pool in memory.pools:
        y_offset = y_offsets[pool]

        colors: set[str] = set()
        for alloc in pool.allocations:
            color = _get_allocation_color(alloc.kind)
            colors.add(color)
            extent = panel.extents.get(id(alloc))
            if extent is not None:
                _draw_allocation(ax, alloc, y_offset, color, extent)
        _draw_pool_background(ax, y_offset, pool.size, colors)

    # Draw used-size, declared-size, and extra capacity lines
    limits: dict[str, int] = {"used": memory.used_size}
    if memory.size is not None:
        limits["size"] = memory.size
    for label, per_memory_capacity in capacities.items():
        if memory.id in per_memory_capacity:
            limits[label] = per_memory_capacity[memory.id]

    _draw_limit_lines(ax, limits)


def _visualize_system(
    system: System,
    path: Path | str | None,
    capacities: dict[str, dict[IdType, int]],
    view: Literal["panel", "lanes"],
    max_lanes: int | None,
) -> None:
    if view == "lanes":
        panels, caveat = _lane_panels(system, max_lanes)
    else:
        panels, caveat = _projection_panels(system)
    if not panels:
        raise ValueError(f"Nothing to plot: system {system.id!r} has no memories")
    fig_height = max(9, len(panels) * 6)
    fig_width = 12
    fig, axs = plt.subplots(
        nrows=len(panels),
        ncols=1,
        figsize=(fig_width, fig_height),
        layout="constrained",
    )
    axs = [axs] if len(panels) == 1 else axs

    y_limits = _get_y_limits(system)
    y_offsets = _get_y_offsets(system)

    for ax, panel in zip(axs, panels, strict=True):
        _draw_panel(
            ax,
            panel,
            y_limits[panel.memory],
            y_offsets[panel.memory],
            capacities,
        )

    if caveat is not None:
        # Both projections of a partial order are sound but lossy: visible
        # collisions are real, but some conflicts need not be visible.
        fig.suptitle(caveat, fontsize=8)
    _add_legend(fig)

    if path is None:
        plt.show()
    else:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, bbox_inches="tight")

    plt.close(fig)


def plot_allocation(
    entity: System | Memory | Pool | Sequence[Allocation],
    path: Path | str | None = None,
    capacities: dict[str, dict[IdType, int]] | None = None,
    view: Literal["panel", "lanes"] = "panel",
    max_lanes: int | None = None,
) -> None:
    """Plot an allocated entity: `path=None` displays the figure, `path=...` saves it.

    Accepts a System, Memory, Pool, or raw sequence of Allocations
    (plotted as a single pool). `capacities` draws extra horizontal limit
    lines, keyed by label then memory id. `view="panel"` draws each
    memory once over a happens-before-monotone virtual time (exact for
    scalar or linearizable lifetimes, else a sound projection annotated
    with its conflict coverage); `view="lanes"` draws one subplot per
    thread's local-time projection, capped to the top `max_lanes` threads
    by peak definitely-live occupancy. Both views only ever show genuine
    conflicts as temporal overlaps. Raises `ImportError` without
    matplotlib.
    """
    if view not in ("panel", "lanes"):
        raise ValueError(f'view must be "panel" or "lanes", got {view!r}')
    if max_lanes is not None and max_lanes < 1:
        raise ValueError(f"max_lanes must be positive, got {max_lanes}")
    if max_lanes is not None and view != "lanes":
        raise ValueError('max_lanes requires view="lanes"')
    if not HAS_MATPLOTLIB:
        require_optional("matplotlib", "visualization")

    if not isinstance(entity, System | Memory | Pool):
        entity = Pool.from_allocations(entity)

    if isinstance(entity, Pool):
        entity = Memory(id=f"memory_{entity.id}", pools=(entity,))

    if isinstance(entity, Memory):
        entity = System(id=f"system_{entity.id}", memories=(entity,))

    _visualize_system(
        system=entity,
        path=path,
        capacities=capacities or {},
        view=view,
        max_lanes=max_lanes,
    )
