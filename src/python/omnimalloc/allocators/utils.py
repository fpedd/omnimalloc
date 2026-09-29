#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Final

from .base import BaseAllocator
from .omni import OmniAllocator

DEFAULT_ALLOCATOR: Final[str] = OmniAllocator.name()


def available_allocators() -> tuple[str, ...]:
    """Return a tuple of available allocator names (including user-registered)."""
    return tuple(BaseAllocator.registry().keys())


def ensure_seed(seed: int) -> None:
    """Raise ValueError unless seed fits the native kernels' uint64 seeds."""
    if not 0 <= seed < 2**64:
        raise ValueError(f"seed must be in [0, 2**64), got {seed}")
