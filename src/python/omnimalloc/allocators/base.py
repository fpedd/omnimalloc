#
# SPDX-License-Identifier: Apache-2.0
#

from abc import abstractmethod
from typing import TYPE_CHECKING, Final

from omnimalloc.common.registry import Registered

if TYPE_CHECKING:
    from omnimalloc.primitives import Allocation

# Shared wall-clock budget for every time-bounded allocator (seconds).
DEFAULT_TIMEOUT: Final[float] = 3.0


def require_unique_ids(allocations: tuple["Allocation", ...]) -> None:
    """Raise if any allocation id repeats; id-keyed placement assumes uniqueness."""
    seen: set[object] = set()
    for alloc in allocations:
        if alloc.id in seen:
            raise ValueError("allocation ids must be unique")
        seen.add(alloc.id)


class BaseAllocator(Registered):
    """Base class for allocators with automatic registry."""

    @abstractmethod
    def allocate(
        self, allocations: tuple["Allocation", ...]
    ) -> tuple["Allocation", ...]:
        """Run the allocator on the given allocations."""
        ...

    def reset(self) -> None:
        """Optional: reset allocator state/config. Override if needed."""
        ...
