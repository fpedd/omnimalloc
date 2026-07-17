#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Final

from .base import BaseSource
from .generator import RandomSource

DEFAULT_SOURCE: Final[str] = RandomSource.name()


def available_sources() -> tuple[str, ...]:
    """Return a tuple of available source names (including user-registered)."""
    return tuple(BaseSource.registry().keys())
