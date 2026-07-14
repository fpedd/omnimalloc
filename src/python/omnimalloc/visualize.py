#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from collections import defaultdict
from pathlib import Path
from typing import Final

from omnimalloc.common.optional import require_optional
from omnimalloc.primitives import Allocation, BufferKind, IdType, Memory, Pool, System
from omnimalloc.primitives.vector_clock import ensure_uniform_dim, time_components

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


KIND_COLOR_MAP: Final[dict[BufferKind, str]] = {
    BufferKind.WORKSPACE: "C0",
    BufferKind.CONSTANT: "C1",
    BufferKind.INPUT: "C2",
    BufferKind.OUTPUT: "C3",
}

LANE_CAVEAT: Final[str] = (
    "Lanes show each thread's local timeline: overlaps within a lane are real "
    "conflicts, but cross-thread conflicts may not be visible anywhere; "
    "validate_allocation() is the authority."
)


def _get_allocation_color(kind: BufferKind | None) -> str:
    if kind is None:
        kind = BufferKind.WORKSPACE
    if kind not in KIND_COLOR_MAP:
        raise ValueError(f"Unknown allocation kind: {kind}")
    return KIND_COLOR_MAP[kind]


def _memory_dim(memory: Memory) -> int:
    # Dimension is uniform per pool (the validate.py contract), but pools of
    # one memory may mix, e.g. after linearizing one pool; lanes cover the max.
    return max(
        (ensure_uniform_dim(pool.allocations) for pool in memory.pools), default=1
    )


def _lane_extent(alloc: Allocation, lane: int) -> tuple[int, int]:
    """Project an allocation's lifetime onto one thread's local timeline."""
    return time_components(alloc.start)[lane], time_components(alloc.end)[lane]


def _get_x_limits(system: System) -> dict[int, tuple[int, int]]:
    """Per-lane x-limits, shared across memories so aligned lanes compare."""
    max_ends: dict[int, int] = {}
    for memory in system.memories:
        for pool in memory.pools:
            for alloc in pool.allocations:
                for lane, end in enumerate(time_components(alloc.end)):
                    max_ends[lane] = max(max_ends.get(lane, 0), end)
    # Clamp to at least 1 so empty lanes keep a non-degenerate x-axis
    return {lane: (0, max(end, 1)) for lane, end in max_ends.items()}


def _get_y_limits(system: System) -> dict[Memory, tuple[int, int]]:
    limits: dict[Memory, tuple[int, int]] = {}
    for memory in system.memories:
        size = memory.size
        used = memory.used_size

        if size is None:
            # No size limit defined, scale to 1.2x used
            y_limit = used * 1.2

        elif used > size:
            # Usage exceeds size, scale to 1.2x usage
            y_limit = used * 1.2

        elif used >= size * 0.5:
            # Usage is 50-100% of size, use size as limit
            y_limit = size

        else:
            # Usage below 50% of size, scale to 2x usage
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
    ax: Axes, alloc: Allocation, offset: int, color: str, lane: int = 0
) -> None:
    """Draw a single allocation rectangle, projected onto the given lane."""
    assert alloc.offset is not None
    if lane >= alloc.dim:
        return  # Lower-dim allocation in a mixed memory; no such thread lane
    start, end = _lane_extent(alloc, lane)
    if start == end:
        return  # No local time on this thread; not visible in this lane
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
    """Draw horizontal lines with annotations for memory limits."""
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


def _set_axes_labels(
    ax: Axes,
    memory: Memory,
    memory_size: int | None,
    num_pools: int,
    lane: int,
    dim: int,
) -> None:
    size_str = _format_bytes(memory_size) if memory_size is not None else "Unknown Size"
    threads_str = f", {dim} threads" if dim > 1 else ""
    if lane == 0:
        ax.set_title(f"{memory.id} ({size_str}, {num_pools} pools{threads_str})")
    ax.set_xlabel("Time (Step)" if dim == 1 else f"Thread {lane} Time (Step)")


def _set_axes_limits(
    ax: Axes,
    x_limits: tuple[int, int],
    y_limits: tuple[int, int],
    memory_size: int | None,
) -> None:
    """Set axis limits and add scaling notice if needed."""
    ax.set_xlim(x_limits)
    ax.set_ylim(y_limits)
    if memory_size is not None and y_limits[1] < memory_size:
        ax.text(
            0.02,
            0.98,
            "Y-axis scaled down for improved readability",
            transform=ax.transAxes,
            va="top",
            fontsize=8,
            alpha=0.7,
        )


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


# TODO(fpedd): Add a pools size descriptor on the right side of each pool


def _visualize_system(
    system: System,
    file_path: Path | str | None,
    show_inline: bool,
    memory_limits: dict[str, dict[IdType, int]],
) -> Path | None:
    dims = {memory: _memory_dim(memory) for memory in system.memories}
    lanes = [
        (memory, lane) for memory in system.memories for lane in range(dims[memory])
    ]
    fig_height = max(9, len(lanes) * 6)
    fig_width = 12
    fig, axs = plt.subplots(
        nrows=len(lanes),
        ncols=1,
        figsize=(fig_width, fig_height),
        layout="constrained",
    )
    axs = [axs] if len(lanes) == 1 else axs

    x_limits = _get_x_limits(system)
    y_limits = _get_y_limits(system)
    y_offsets = _get_y_offsets(system)

    for ax, (memory, lane) in zip(axs, lanes, strict=True):
        memory_y_limits = y_limits[memory]
        _set_axes_labels(ax, memory, memory.size, len(memory.pools), lane, dims[memory])
        _set_axes_limits(ax, x_limits.get(lane, (0, 1)), memory_y_limits, memory.size)
        _set_axes_ticks(ax, memory_y_limits[1])

        for pool in memory.pools:
            y_offset = y_offsets[memory][pool]

            colors: set[str] = set()
            for alloc in pool.allocations:
                color = _get_allocation_color(alloc.kind)
                _draw_allocation(ax, alloc, y_offset, color, lane)
                colors.add(color)
            _draw_pool_background(ax, y_offset, pool.size, colors)

        # Draw memory limit lines
        limits: dict[str, int] = {"used": memory.used_size}
        if memory.size is not None:
            limits["size"] = memory.size
        for limit_type, memory_id_to_limit in memory_limits.items():
            if memory.id in memory_id_to_limit:
                limits[limit_type] = memory_id_to_limit[memory.id]

        _draw_limit_lines(ax, limits)

    if any(dim > 1 for dim in dims.values()):
        # Lanes are per-thread projections of a partial order: same-lane
        # collisions are real, but cross-thread conflicts need not be visible.
        fig.suptitle(LANE_CAVEAT, fontsize=8)
    _add_legend(fig)

    if show_inline:
        plt.show()

    if file_path is not None:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(file_path, bbox_inches="tight")

    plt.close(fig)

    return file_path


def _canonicalize(system: System) -> System:
    """Reassign allocation IDs sequentially for cleaner visualization."""

    def _id_sort_key(id_val: IdType) -> tuple[int, int | str]:
        return (0, id_val) if isinstance(id_val, int) else (1, id_val)

    def _alloc_sort_key(alloc: Allocation) -> tuple[object, ...]:
        # Lexicographic on the (possibly vector) start, then original id
        return time_components(alloc.start), _id_sort_key(alloc.id)

    # Collect all allocations and assign sequential IDs
    all_allocations = [
        alloc
        for memory in system.memories
        for pool in memory.pools
        for alloc in pool.allocations
    ]

    all_allocations.sort(key=_alloc_sort_key)

    # Create mapping from old allocation to new ID
    alloc_to_new_id = {
        id(alloc): new_id for new_id, alloc in enumerate(all_allocations)
    }

    # Rebuild with new IDs
    canonical_memories = tuple(
        Memory(
            id=memory.id,
            size=memory.size,
            pools=tuple(
                Pool(
                    id=pool.id,
                    offset=pool.offset,
                    allocations=tuple(
                        Allocation(
                            id=alloc_to_new_id[id(alloc)],
                            size=alloc.size,
                            start=alloc.start,
                            end=alloc.end,
                            offset=alloc.offset,
                            kind=alloc.kind,
                        )
                        for alloc in sorted(pool.allocations, key=_alloc_sort_key)
                    ),
                )
                for pool in sorted(memory.pools, key=lambda p: _id_sort_key(p.id))
            ),
        )
        for memory in sorted(system.memories, key=lambda m: _id_sort_key(m.id))
    )

    return System(id=system.id, memories=canonical_memories)


def plot_allocation(
    entity: System | Memory | Pool,
    file_path: Path | str | None = None,
    show_inline: bool = False,
    canonicalize: bool = False,
    memory_limits: dict[str, dict[IdType, int]] | None = None,
) -> Path | None:
    """Plot an allocated entity (System, Memory, or Pool).

    Args:
        entity: The entity to plot.
        file_path: Optional path to save the plot.
        show_inline: Whether to display inline (for notebooks).
        canonicalize: Whether to canonicalize IDs for cleaner visualization.
        memory_limits: Optional dict specifying custom memory limits
                       for each memory in the system.

    Returns:
        Path to the saved file, or None if not saved.
    """
    if not HAS_MATPLOTLIB:
        require_optional("matplotlib", "visualization")

    if isinstance(entity, Pool):
        entity = Memory(id=f"memory_{entity.id}", pools=(entity,))

    if isinstance(entity, Memory):
        entity = System(id=f"system_{entity.id}", memories=(entity,))

    if canonicalize:
        entity = _canonicalize(entity)

    return _visualize_system(
        system=entity,
        file_path=file_path,
        show_inline=show_inline,
        memory_limits=memory_limits or {},
    )
