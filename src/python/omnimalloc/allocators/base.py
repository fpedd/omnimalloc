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
    from omnimalloc.primitives import Allocation


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
        """Place the allocations; the result keeps input order and every pin."""
        self._ensure_preconditions(allocations)
        if not allocations:
            return allocations
        return self._finish(allocations, self._allocate(allocations))

    def _ensure_preconditions(self, allocations: tuple["Allocation", ...]) -> None:
        """Shared entry contract: unique ids, one clock dim, supported, pins apart."""
        ensure_unique_ids(allocations, "allocation")
        uniform_dim(allocations)
        self.ensure_supported(allocations)
        self._ensure_pins_placeable(allocations)

    def _finish(
        self, allocations: tuple["Allocation", ...], placed: tuple["Allocation", ...]
    ) -> tuple["Allocation", ...]:
        """Shared exit contract: `placed` in input order, all placed, pins put.

        A dropped or padded result would win when ranked by peak, so refuse it.
        """
        if len(placed) != len(allocations):
            raise ValueError(f"{self.name()} returned a different allocation set")
        by_id = {alloc.id: alloc for alloc in placed}
        ordered = []
        for alloc in allocations:
            result = by_id.get(alloc.id)
            if result is None:
                raise ValueError(f"{self.name()} returned a different allocation set")
            if result.offset is None:
                raise ValueError(f"{self.name()} left allocation {alloc.id!r} unplaced")
            if alloc.offset is not None and result.offset != alloc.offset:
                raise ValueError(
                    f"{self.name()} moved pinned allocation {alloc.id!r} "
                    f"from {alloc.offset} to {result.offset}"
                )
            ordered.append(result)
        return tuple(ordered)

    @abstractmethod
    def _allocate(
        self, allocations: tuple["Allocation", ...]
    ) -> tuple["Allocation", ...]:
        """Place the validated, non-empty allocations. Implemented by subclasses."""
        ...

    def supports(self, allocations: tuple["Allocation", ...]) -> bool:
        """Whether `allocate` accepts these allocations' clock dims and pins."""
        try:
            self.ensure_supported(allocations)
        except ValueError:
            return False
        return True

    def ensure_supported(self, allocations: tuple["Allocation", ...]) -> None:
        """Raise if these allocations' clock dimensions or pins aren't supported."""
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
