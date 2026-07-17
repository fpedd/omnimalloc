#
# SPDX-License-Identifier: Apache-2.0
#


def ensure_positive(value: float, name: str) -> None:
    """Raise ValueError if value is not positive."""
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")


def ensure_non_negative(value: float, name: str) -> None:
    """Raise ValueError if value is not non-negative."""
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value}")
