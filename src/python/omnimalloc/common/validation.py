#
# SPDX-License-Identifier: Apache-2.0
#


def ensure_positive(value: float | None, name: str, allow_none: bool = False) -> None:
    """Raise ValueError unless value is positive, or None where that disables it."""
    if (value is None and not allow_none) or (value is not None and value <= 0):
        allowed = "positive or None" if allow_none else "positive"
        raise ValueError(f"{name} must be {allowed}, got {value}")


def ensure_non_negative(
    value: float | None, name: str, allow_none: bool = False
) -> None:
    """Raise ValueError unless value is non-negative, or None where that disables it."""
    if (value is None and not allow_none) or (value is not None and value < 0):
        allowed = "non-negative or None" if allow_none else "non-negative"
        raise ValueError(f"{name} must be {allowed}, got {value}")
