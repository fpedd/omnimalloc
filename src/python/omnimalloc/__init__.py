#
# SPDX-License-Identifier: Apache-2.0
#

from importlib.metadata import version

__version__ = version("omnimalloc")

from ._allocate import allocate as allocate
from .allocators import OmniAllocator as OmniAllocator
from .analysis import conflicts as conflicts
from .analysis import pressure as pressure
from .analysis import try_linearize as try_linearize
from .io import load_allocation as load_allocation
from .io import save_allocation as save_allocation
from .primitives import Allocation as Allocation
from .primitives import AllocationKind as AllocationKind
from .primitives import IdType as IdType
from .primitives import Memory as Memory
from .primitives import Pool as Pool
from .primitives import System as System
from .primitives import TimePoint as TimePoint
from .validate import validate_allocation as validate_allocation
from .visualize import plot_allocation as plot_allocation
