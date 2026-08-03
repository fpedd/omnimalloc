#
# SPDX-License-Identifier: Apache-2.0
#

from abc import abstractmethod
from typing import TYPE_CHECKING, ClassVar

from omnimalloc._cpp import find_collision
from omnimalloc.analysis.clock import uniform_dim
from omnimalloc.common.registry import Registered
from omnimalloc.primitives.utils import ensure_unique_ids

if TYPE_CHECKING:
    from omnimalloc.primitives import Allocation, IdType


class BaseAllocator(Registered):
    """Base class for allocators with automatic registry."""

    # Registry keys drop the class-role token: GreedyBySizeAllocator
    # registers as "greedy_by_size".
    _strip_suffix: ClassVar[str] = "Allocator"

    # True for allocators that consume only the pairwise conflict relation and
    # thus accept vector-clock lifetimes. Subclasses that add logic reading
    # scalar start/end directly must declare this False again.
    supports_vector_time: ClassVar[bool] = False

    # True for allocators that honor an incoming offset as a pin: the
    # allocation keeps that address and the rest pack around it. Subclasses that
    # re-place everything leave it False, and `allocate` rejects pinned input.
    supports_pinned: ClassVar[bool] = False

    def __repr__(self) -> str:
        kwargs = ", ".join(
            f"{key.lstrip('_')}={value!r}" for key, value in vars(self).items()
        )
        return f"{type(self).__name__}({kwargs})"

    def allocate(
        self, allocations: tuple["Allocation", ...]
    ) -> tuple["Allocation", ...]:
        """Validate shared preconditions, then run the allocator."""
        ensure_unique_ids(allocations, "allocation")
        uniform_dim(allocations)
        self.ensure_supported(allocations)
        if not allocations:
            return allocations
        pins = {alloc.id: alloc.offset for alloc in allocations if alloc.is_allocated}
        if pins:
            self._ensure_pins_placeable(allocations)
        placed = self._allocate(allocations)
        self._ensure_placed(placed, pins)
        return placed

    @abstractmethod
    def _allocate(
        self, allocations: tuple["Allocation", ...]
    ) -> tuple["Allocation", ...]:
        """Place the validated, non-empty allocations. Implemented by subclasses."""
        ...

    def supports(self, allocations: tuple["Allocation", ...]) -> bool:
        """Whether this allocator accepts the allocations' clock dimensions."""
        return self.supports_vector_time or all(alloc.dim == 1 for alloc in allocations)

    def ensure_supported(self, allocations: tuple["Allocation", ...]) -> None:
        """Raise if these allocations' clock dimensions or pins aren't supported."""
        if not self.supports(allocations):
            max_dim = max(alloc.dim for alloc in allocations)
            raise ValueError(
                f"{self.name()} requires scalar (interval) lifetimes, "
                f"got {max_dim}-dim vector clocks"
            )
        if not self.supports_pinned and any(
            alloc.is_allocated for alloc in allocations
        ):
            raise ValueError(
                f"{self.name()} cannot honor pinned offsets; clear them first "
                f"or pick an allocator whose supports_pinned is True"
            )

    def _ensure_pins_placeable(self, allocations: tuple["Allocation", ...]) -> None:
        """Pins that already collide admit no placement, so say so up front."""
        pinned = tuple(alloc for alloc in allocations if alloc.is_allocated)
        collision = find_collision(pinned)
        if collision is not None:
            first, second = collision
            raise ValueError(
                f"pinned allocations {pinned[first].id!r} and "
                f"{pinned[second].id!r} already collide"
            )

    def _ensure_placed(
        self, placed: tuple["Allocation", ...], pins: dict["IdType", int | None]
    ) -> None:
        """Every allocation comes back placed, and every pin comes back put."""
        unplaced = next((alloc for alloc in placed if alloc.offset is None), None)
        if unplaced is not None:
            raise ValueError(f"{self.name()} left allocation {unplaced.id!r} unplaced")
        if not pins:
            return
        for alloc in placed:
            pinned_at = pins.get(alloc.id)
            if pinned_at is not None and pinned_at != alloc.offset:
                raise ValueError(
                    f"{self.name()} moved pinned allocation {alloc.id!r} "
                    f"from {pinned_at} to {alloc.offset}"
                )
