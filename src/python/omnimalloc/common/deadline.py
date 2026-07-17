#
# SPDX-License-Identifier: Apache-2.0
#

import math
import time


def ensure_valid_timeout(timeout: float | None) -> None:
    """Raise ValueError if timeout is not positive or None (disabled)."""
    if timeout is not None and not (math.isfinite(timeout) and timeout > 0):
        raise ValueError(
            f"timeout must be positive or None, got {timeout}; "
            "use None to disable the deadline"
        )


def ensure_valid_budget(budget: int | None, name: str = "work_budget") -> None:
    """Raise ValueError if budget is not non-negative or None (disabled)."""
    if budget is not None and budget < 0:
        raise ValueError(f"{name} must be non-negative, got {budget}")


def make_deadline(timeout: float | None) -> float | None:
    """Absolute time.monotonic() deadline, or None when the budget is disabled."""
    return None if timeout is None else time.monotonic() + timeout


def deadline_remaining(deadline: float | None) -> float | None:
    """Seconds left on the budget (0.0 once expired), or None when disabled."""
    return None if deadline is None else max(0.0, deadline - time.monotonic())


def deadline_expired(deadline: float | None) -> bool:
    """Whether the budget has expired (False when disabled)."""
    return deadline is not None and time.monotonic() >= deadline
