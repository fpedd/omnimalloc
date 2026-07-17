#
# SPDX-License-Identifier: Apache-2.0
#

import os


def ensure_valid_num_threads(num_threads: int | None) -> None:
    """Raise ValueError if num_threads is not positive or None (disabled)."""
    if num_threads is not None and num_threads < 1:
        raise ValueError(
            f"num_threads must be positive or None, got {num_threads}; "
            "use None for all cores"
        )


def resolve_num_threads(num_threads: int | None) -> int:
    """Worker count for a parallel section; None resolves to all cores."""
    ensure_valid_num_threads(num_threads)
    return num_threads if num_threads is not None else os.cpu_count() or 1
