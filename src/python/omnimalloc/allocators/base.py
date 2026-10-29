#
# SPDX-License-Identifier: Apache-2.0
#

import inspect
from abc import abstractmethod
from typing import TYPE_CHECKING, ClassVar

from omnimalloc._cpp import find_collision
from omnimalloc.analysis._clock import uniform_dim
from omnimalloc.common.registry import Registered
from omnimalloc.primitives.utils import ensure_unique_ids

if TYPE_CHECKING:
    from omnimalloc.primitives import Allocation, IdType


def _misaligned(alloc: "Allocation") -> bool:
    return alloc.offset is not None and alloc.offset % alloc.alignment != 0


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

    # True for allocators that place every allocation at a multiple of its
    # `alignment`; `allocate` rejects any alignment above 1 for the rest.
    supports_alignment: ClassVar[bool] = False

    def __repr__(self) -> str:
        params = inspect.signature(type(self).__init__).parameters
        kwargs = ", ".join(
            f"{key.lstrip('_')}={value!r}"
            for key, value in vars(self).items()
            if key.lstrip("_") in params
        )
        return f"{type(self).__name__}({kwargs})"

    def allocate(
        self, allocations: tuple["Allocation", ...]
    ) -> tuple["Allocation", ...]:
        """Validate shared preconditions, then run the allocator."""
        pins = self._ensure_preconditions(allocations)
        if not allocations:
            return allocations
        placed = self._allocate(allocations)
        self._ensure_postconditions(allocations, placed, pins)
        return placed

    def _ensure_preconditions(
        self, allocations: tuple["Allocation", ...]
    ) -> dict["IdType", int | None]:
        """Shared entry contract; returns the pinned offsets keyed by id."""
        ensure_unique_ids(allocations, "allocation")
        uniform_dim(allocations)
        self.ensure_supported(allocations)
        pins = {alloc.id: alloc.offset for alloc in allocations if alloc.is_allocated}
        if pins:
            self._ensure_pins_placeable(allocations)
        return pins

    def _ensure_postconditions(
        self,
        allocations: tuple["Allocation", ...],
        placed: tuple["Allocation", ...],
        pins: dict["IdType", int | None],
    ) -> None:
        """Shared exit contract: same set, fully placed, pins untouched."""
        self._ensure_same_set(allocations, placed)
        self._ensure_placed(placed, pins)

    @abstractmethod
    def _allocate(
        self, allocations: tuple["Allocation", ...]
    ) -> tuple["Allocation", ...]:
        """Place the validated, non-empty allocations. Implemented by subclasses."""
        ...

    def supports(self, allocations: tuple["Allocation", ...]) -> bool:
        """Whether `allocate` accepts these allocations' dims, pins, alignments."""
        try:
            self.ensure_supported(allocations)
        except ValueError:
            return False
        return True

    def ensure_supported(self, allocations: tuple["Allocation", ...]) -> None:
        """Raise if these allocations' dims, pins, or alignments aren't supported."""
        if not self.supports_vector_time and any(
            alloc.dim != 1 for alloc in allocations
        ):
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
        if not self.supports_alignment and any(
            alloc.alignment != 1 for alloc in allocations
        ):
            raise ValueError(
                f"{self.name()} cannot honor alignments; pick an allocator "
                f"whose supports_alignment is True"
            )

    def _ensure_pins_placeable(self, allocations: tuple["Allocation", ...]) -> None:
        """Pins that already collide admit no placement, so say so up front."""
        pinned = tuple(alloc for alloc in allocations if alloc.is_allocated)
        misaligned = next((a for a in pinned if _misaligned(a)), None)
        if misaligned is not None:
            raise ValueError(
                f"pinned allocation {misaligned.id!r} at offset "
                f"{misaligned.offset} breaks its alignment {misaligned.alignment}"
            )
        collision = find_collision(pinned)
        if collision is not None:
            first, second = collision
            raise ValueError(
                f"pinned allocations {pinned[first].id!r} and "
                f"{pinned[second].id!r} already collide"
            )

    def _ensure_same_set(
        self, allocations: tuple["Allocation", ...], placed: tuple["Allocation", ...]
    ) -> None:
        """Refuse a dropped or padded result; ranked by peak, it would win."""
        placed_ids = {alloc.id for alloc in placed}
        wanted_ids = {alloc.id for alloc in allocations}
        if len(placed) != len(allocations) or placed_ids != wanted_ids:
            raise ValueError(f"{self.name()} returned a different allocation set")

    def _ensure_placed(
        self, placed: tuple["Allocation", ...], pins: dict["IdType", int | None]
    ) -> None:
        """Every allocation comes back placed, aligned, and pins come back put."""
        unplaced = next((alloc for alloc in placed if alloc.offset is None), None)
        if unplaced is not None:
            raise ValueError(f"{self.name()} left allocation {unplaced.id!r} unplaced")
        misaligned = next((a for a in placed if _misaligned(a)), None)
        if misaligned is not None:
            raise ValueError(
                f"{self.name()} placed allocation {misaligned.id!r} at offset "
                f"{misaligned.offset}, breaking its alignment {misaligned.alignment}"
            )
        if not pins:
            return
        for alloc in placed:
            pinned_at = pins.get(alloc.id)
            if pinned_at is not None and pinned_at != alloc.offset:
                raise ValueError(
                    f"{self.name()} moved pinned allocation {alloc.id!r} "
                    f"from {pinned_at} to {alloc.offset}"
                )
