#
# SPDX-License-Identifier: Apache-2.0
#

"""Static memory allocation: place buffers with temporal bounds, minimize peak."""

from importlib.metadata import version as _version

__version__ = _version("omnimalloc")

from ._allocate import allocate as allocate
from .allocators import available_allocators as available_allocators
from .common.parallel import max_threads as max_threads
from .common.parallel import set_max_threads as set_max_threads
from .primitives import Allocation as Allocation
from .primitives import AllocationKind as AllocationKind
from .primitives import IdType as IdType
from .primitives import Memory as Memory
from .primitives import Pool as Pool
from .primitives import System as System
from .primitives import TimePoint as TimePoint
from .primitives import VectorClock as VectorClock
from .validate import validate_allocation as validate_allocation
from .visualize import plot_allocation as plot_allocation
