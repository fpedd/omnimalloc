#
# SPDX-License-Identifier: Apache-2.0
#

import os

from omnimalloc._cpp import max_threads as _max_threads
from omnimalloc._cpp import set_max_threads as _set_max_threads

from .validation import ensure_positive


def set_max_threads(value: int | None) -> None:
    """Cap the workers this library will use, anywhere; None lifts the cap.

    Covers the native kernels and the worker pools alike. The kernels spawn per
    call, so without the default 8, N callers put N times the cores in flight.
    """
    if value is not None and value < 1:
        raise ValueError(f"max threads must be positive or None, got {value}")
    _set_max_threads(0 if value is None else value)


def max_threads() -> int:
    """Workers in force, never above what this process may actually use."""
    return _max_threads()


def available_cores() -> int:
    """Cores this process may actually run on, not the ones the machine has.

    Under an affinity mask or a CPU-limited container `os.cpu_count()` still
    reports the whole machine, oversubscribing every pool by the ratio.
    """
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0)) or 1
    return os.cpu_count() or 1


def resolve_num_threads(num_threads: int | None) -> int:
    """Worker count for a parallel section; None resolves to the ceiling.

    An explicit count is taken as given; `None` defers to `max_threads`, so one
    setting governs the worker pools and the native kernels together.
    """
    ensure_positive(num_threads, "num_threads", allow_none=True)
    return num_threads if num_threads is not None else max_threads()
