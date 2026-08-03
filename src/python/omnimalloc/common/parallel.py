#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import max_threads as _max_threads
from omnimalloc._cpp import set_max_threads as _set_max_threads
from omnimalloc._cpp import usable_cores as _usable_cores


def set_max_threads(value: int | None) -> None:
    """Cap the workers this library will use, anywhere; None lifts the cap.

    Covers the native kernels and the process pools alike. The kernels spawn per
    call, so without the default 8, N callers put N times the cores in flight.
    """
    if value is not None and value < 1:
        raise ValueError(f"max threads must be positive or None, got {value}")
    _set_max_threads(0 if value is None else value)


def max_threads() -> int:
    """Workers in force, never above what this process may actually use."""
    return _max_threads()


def adopt_max_threads(value: int) -> None:
    """Worker-process entry point: take `value` as this process's ceiling.

    The ceiling lives in native process-global state, which a forked worker
    inherits and a spawned one does not, so a pool passes it down explicitly.
    """
    set_max_threads(value)


def ensure_valid_num_threads(num_threads: int | None) -> None:
    """Raise ValueError if num_threads is not positive or None (disabled)."""
    if num_threads is not None and num_threads < 1:
        raise ValueError(
            f"num_threads must be positive or None, got {num_threads}; "
            "use None for all cores"
        )


def available_cores() -> int:
    """Cores this process may actually run on, not the ones the machine has.

    Under an affinity mask or a CPU-limited container `os.cpu_count()` still
    reports the whole machine, oversubscribing every pool by the ratio.
    """
    return _usable_cores()


def resolve_num_threads(num_threads: int | None) -> int:
    """Worker count for a parallel section; None resolves to the ceiling.

    An explicit count is taken as given; `None` defers to `max_threads`, so one
    setting governs the process pools and the native kernels together.
    """
    ensure_valid_num_threads(num_threads)
    return num_threads if num_threads is not None else max_threads()
