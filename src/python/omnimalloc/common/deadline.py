#
# SPDX-License-Identifier: Apache-2.0
#

import math
import time

# Python mirror of the C++ deadline helpers (src/cpp/common/deadline.hpp):
# one spelling of the shared timeout convention, where None disables the
# budget. The work-budget validator lives here too — same None-disables shape.


def ensure_valid_timeout(timeout: float | None) -> None:
    if timeout is not None and not (math.isfinite(timeout) and timeout > 0):
        raise ValueError(
            f"timeout must be positive or None, got {timeout}; "
            "use None to disable the deadline"
        )


def ensure_valid_budget(budget: int | None, name: str = "work_budget") -> None:
    if budget is not None and budget < 0:
        raise ValueError(f"{name} must be non-negative, got {budget}")


def make_deadline(timeout: float | None) -> float | None:
    """Absolute time.monotonic() deadline, or None when the budget is disabled."""
    return None if timeout is None else time.monotonic() + timeout


def deadline_remaining(deadline: float | None) -> float | None:
    """Seconds left on the budget (0.0 once expired), or None when disabled."""
    return None if deadline is None else max(0.0, deadline - time.monotonic())


def deadline_expired(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline
