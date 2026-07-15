#
# SPDX-License-Identifier: Apache-2.0
#

from abc import abstractmethod
from typing import TYPE_CHECKING, ClassVar, Final

from omnimalloc.common.registry import Registered
from omnimalloc.primitives.utils import ensure_unique_ids
from omnimalloc.primitives.vector_clock import ensure_uniform_dim

if TYPE_CHECKING:
    from omnimalloc.primitives import Allocation

# Shared wall-clock budget for every time-bounded allocator (seconds).
DEFAULT_TIMEOUT: Final[float] = 3.0


class BaseAllocator(Registered):
    """Base class for allocators with automatic registry."""

    # True for allocators that consume only the pairwise conflict relation and
    # thus accept vector-clock lifetimes. Subclasses that add logic reading
    # scalar start/end directly must declare this False again.
    supports_vector_time: ClassVar[bool] = False

    def allocate(
        self, allocations: tuple["Allocation", ...]
    ) -> tuple["Allocation", ...]:
        """Validate shared preconditions, then run the allocator."""
        ensure_unique_ids(allocations)
        ensure_uniform_dim(allocations)
        self.ensure_supported(allocations)
        if not allocations:
            return allocations
        return self._allocate(allocations)

    @abstractmethod
    def _allocate(
        self, allocations: tuple["Allocation", ...]
    ) -> tuple["Allocation", ...]:
        """Place the validated, non-empty allocations. Implemented by subclasses."""
        ...

    def reset(self) -> None:
        """Optional: reset allocator state/config. Override if needed."""
        ...

    def supports(self, allocations: tuple["Allocation", ...]) -> bool:
        """Whether this allocator accepts the allocations' clock dimensions."""
        return self.supports_vector_time or all(alloc.dim == 1 for alloc in allocations)

    def ensure_supported(self, allocations: tuple["Allocation", ...]) -> None:
        """Raise if these allocations' clock dimensions aren't supported."""
        if not self.supports(allocations):
            max_dim = max(alloc.dim for alloc in allocations)
            raise ValueError(
                f"{self.name()} requires scalar (interval) lifetimes, "
                f"got {max_dim}-dim vector clocks"
            )
