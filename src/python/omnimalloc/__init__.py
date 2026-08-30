#
# SPDX-License-Identifier: Apache-2.0
#

"""Static memory allocation: place buffers with temporal bounds, minimize peak."""

import typing

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

if typing.TYPE_CHECKING:
    from ._allocate import allocate as allocate
    from .allocators import available_allocators as available_allocators
    from .visualize import plot_allocation as plot_allocation

# Heavy submodules load on first attribute access to keep `import omnimalloc` fast.
_LAZY_EXPORTS: dict[str, str] = {
    "allocate": "._allocate",
    "available_allocators": ".allocators",
    "plot_allocation": ".visualize",
}


def __getattr__(name: str) -> object:
    if name == "__version__":
        from importlib.metadata import version

        value: object = version("omnimalloc")
    elif name in _LAZY_EXPORTS:
        from importlib import import_module

        value = getattr(import_module(_LAZY_EXPORTS[name], __name__), name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value
