#
# SPDX-License-Identifier: Apache-2.0
#

from abc import abstractmethod
from typing import TYPE_CHECKING

from omnimalloc.common.registry import Registered

if TYPE_CHECKING:
    from omnimalloc.primitives import Allocation


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
