#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Final

from .base import BaseSource
from .generator import RandomSource

DEFAULT_SOURCE: Final[str] = RandomSource.name()


def get_available_sources() -> tuple[str, ...]:
    """Return a tuple of available source names (including user-registered)."""
    return tuple(BaseSource.registry().keys())


def get_default_source() -> str:
    """Return the name of the default source."""
    return DEFAULT_SOURCE


def get_source_by_name(name: str) -> type[BaseSource]:
    """Get a source class by its registered name."""
    return BaseSource.registry()[name]
