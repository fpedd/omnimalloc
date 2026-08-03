#
# SPDX-License-Identifier: Apache-2.0
#

"""Static memory allocation: place buffers with temporal bounds, minimize peak."""

from importlib.metadata import version as _version

__version__ = _version("omnimalloc")

from ._allocate import allocate
from .primitives import Allocation, Memory, Pool, System
from .validate import validate_allocation
from .visualize import plot_allocation

__all__ = [
    "Allocation",
    "Memory",
    "Pool",
    "System",
    "__version__",
    "allocate",
    "plot_allocation",
    "validate_allocation",
]
