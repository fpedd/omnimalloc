#
# SPDX-License-Identifier: Apache-2.0
#

import math
import time

from .validation import ensure_positive


def ensure_valid_timeout(timeout: float | None) -> None:
    """Raise ValueError if timeout is not positive and finite, or None (disabled)."""
    if timeout is not None and not math.isfinite(timeout):
        raise ValueError(f"timeout must be finite or None, got {timeout}")
    ensure_positive(timeout, "timeout", allow_none=True)


def make_deadline(timeout: float | None) -> float | None:
    """Absolute time.monotonic() deadline, or None when the budget is disabled."""
    return None if timeout is None else time.monotonic() + timeout


def deadline_remaining(deadline: float | None) -> float | None:
    """Seconds left on the budget (0.0 once expired), or None when disabled."""
    return None if deadline is None else max(0.0, deadline - time.monotonic())


def deadline_expired(deadline: float | None) -> bool:
    """Whether the budget has expired (False when disabled)."""
    return deadline is not None and time.monotonic() >= deadline
