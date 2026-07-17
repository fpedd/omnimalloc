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
