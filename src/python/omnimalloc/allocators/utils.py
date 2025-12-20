#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Final

from .base import BaseAllocator
from .greedy import GreedyBySizeAllocator

AVAILABLE_ALLOCATORS: Final[tuple[str, ...]] = tuple(BaseAllocator.registry().keys())
DEFAULT_ALLOCATOR: Final[str] = GreedyBySizeAllocator.name()


def get_available_allocators() -> tuple[str, ...]:
    """Return a tuple of available allocator names."""
    return AVAILABLE_ALLOCATORS


def get_default_allocator() -> str:
    """Return the name of the default allocator."""
    return DEFAULT_ALLOCATOR


def get_allocator_by_name(name: str) -> type[BaseAllocator]:
    """Get an allocator class by its registered name."""
    return BaseAllocator.registry()[name]
