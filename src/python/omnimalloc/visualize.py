#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from collections import defaultdict
from pathlib import Path
from typing import Final

from omnimalloc.common.optional import require_optional
from omnimalloc.common.units import MB
from omnimalloc.primitives import Allocation, BufferKind, IdType, Memory, Pool, System

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

KIND_COLOR_MAP: Final[dict[BufferKind, str]] = {
    BufferKind.WORKSPACE: "C0",
    BufferKind.CONSTANT: "C1",
    BufferKind.INPUT: "C2",
    BufferKind.OUTPUT: "C3",
}


def _get_allocation_color(kind: BufferKind | None) -> str:
    if kind is None:
        kind = BufferKind.WORKSPACE
    if kind not in KIND_COLOR_MAP:
        raise ValueError(f"Unknown allocation kind: {kind}")
    return KIND_COLOR_MAP[kind]


def _get_x_limits(system: System) -> tuple[int, int]:
    max_len = 0
    for memory in system.memories:
        for pool in memory.pools:
            for alloc in pool.allocations:
                max_len = max(max_len, alloc.start + alloc.duration)
    return 0, max_len


def _get_y_limits(system: System) -> dict[Memory, tuple[int, int]]:
    limits: dict[Memory, tuple[int, int]] = {}
    for memory in system.memories:
        size = memory.size
        used = memory.used_size

        if used is None:
            raise ValueError(f"Memory {memory!r} has undefined used size")

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

        limits[memory] = (0, int(y_limit))

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


def _draw_allocation(ax: Axes, alloc: Allocation, offset: int, color: str) -> None:
    """Draw a single allocation rectangle."""
    assert alloc.offset is not None
    y_pos = offset + alloc.offset
    rect = Rectangle(
        xy=(alloc.start, y_pos),
        width=alloc.duration,
        height=alloc.size,
        edgecolor="black",
        facecolor=color,
        alpha=0.5,
    )
    ax.add_patch(rect)
    ax.text(
        alloc.start + alloc.duration / 2,
        y_pos + alloc.size / 2,
        f"{alloc.id}",
        ha="center",
        va="center",
        fontsize=8,
    )


def _draw_pool_background(
    ax: Axes, y_offset: int, pool_size: int, color: str | set[str]
) -> None:
    """Draw background rectangle for allocation pool."""
    if isinstance(color, set):
        color = "gray" if len(color) > 1 else color.pop()
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
            f"{value / MB:.2f} MB\n{name}",
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
    ax.yaxis.set_major_locator(MultipleLocator(tick_size))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x / MB:.1f}"))
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(visible=True, alpha=0.5)


def _set_axes_labels(
    ax: Axes, memory: Memory, memory_size: int | None, num_pools: int
) -> None:
    size_str = (
        f"{memory_size / MB:.1f}MB" if memory_size is not None else "Unknown Size"
    )
    ax.set_title(f"{memory.id} ({size_str}, {num_pools} pools)")
    ax.set_xlabel("Time (Step)")
    ax.set_ylabel("Memory (MB)")


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
    num_memories = len(system.memories)
    fig_height = max(9, num_memories * 6)
    fig_width = 12
    fig, axs = plt.subplots(
        nrows=num_memories,
        ncols=1,
        figsize=(fig_width, fig_height),
        layout="constrained",
    )
    axs = [axs] if num_memories == 1 else axs

    x_limits = _get_x_limits(system)
    y_limits = _get_y_limits(system)
    y_offsets = _get_y_offsets(system)

    for ax, memory in zip(axs, system.memories, strict=True):
        memory_y_limits = y_limits[memory]
        _set_axes_labels(ax, memory, memory.size, len(memory.pools))
        _set_axes_limits(ax, x_limits, memory_y_limits, memory.size)
        _set_axes_ticks(ax, memory_y_limits[1])

        colors: set[str] = set()
        for pool in memory.pools:
            y_offset = y_offsets[memory][pool]

            for alloc in pool.allocations:
                color = _get_allocation_color(alloc.kind)
                _draw_allocation(ax, alloc, y_offset, color)
                colors.add(color)
            _draw_pool_background(ax, y_offset, pool.size, color)

        # Draw memory limit lines
        limits: dict[str, int] = {"used": memory.used_size}
        if memory.size is not None:
            limits["size"] = memory.size
        for limit_type, memory_id_to_limit in memory_limits.items():
            if memory.id in memory_id_to_limit:
                limits[limit_type] = memory_id_to_limit[memory.id]

        _draw_limit_lines(ax, limits)

    _add_legend(fig)

    if show_inline:
        plt.show()

    if file_path is not None:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(file_path, bbox_inches="tight", format="pdf")

    plt.close(fig)

    return file_path


def _canonicalize(system: System) -> System:
    """Reassign allocation IDs sequentially for cleaner visualization."""

    def _id_sort_key(id_val: IdType) -> tuple[int, int | str]:
        return (0, id_val) if isinstance(id_val, int) else (1, id_val)

    # Collect all allocations and assign sequential IDs
    all_allocations = [
        alloc
        for memory in system.memories
        for pool in memory.pools
        for alloc in pool.allocations
    ]

    # Sort by (start, original_id) for stable ordering
    all_allocations.sort(key=lambda a: (a.start, _id_sort_key(a.id)))

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
                        for alloc in sorted(
                            pool.allocations,
                            key=lambda a: (a.start, _id_sort_key(a.id)),
                        )
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
