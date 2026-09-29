#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import max_threads as _max_threads
from omnimalloc._cpp import set_max_threads as _set_max_threads

from .validation import ensure_positive


def set_max_threads(value: int | None) -> None:
    """Cap the workers this library will use, anywhere; None lifts the cap.

    Covers the native kernels and the worker pools alike. The kernels spawn per
    call, so without the default 8, N callers put N times the cores in flight.
    """
    ensure_positive(value, "max threads", allow_none=True)
    _set_max_threads(0 if value is None else value)


def max_threads() -> int:
    """Workers in force, never above what this process may actually use."""
    return _max_threads()


def resolve_num_threads(num_threads: int | None) -> int:
    """Worker count for a parallel section; None resolves to the ceiling.

    An explicit count is taken as given; `None` defers to `max_threads`, so one
    setting governs the worker pools and the native kernels together.
    """
    ensure_positive(num_threads, "num_threads", allow_none=True)
    return num_threads if num_threads is not None else max_threads()
