#
# SPDX-License-Identifier: Apache-2.0
#


def ensure_positive(value: float | None, name: str, allow_none: bool = False) -> None:
    """Raise ValueError unless value is positive, or None where that disables it."""
    if value is None:
        if not allow_none:
            raise ValueError(f"{name} must be positive, got None")
        return
    if value <= 0:
        allowed = "positive or None" if allow_none else "positive"
        raise ValueError(f"{name} must be {allowed}, got {value}")


def ensure_non_negative(
    value: float | None, name: str, allow_none: bool = False
) -> None:
    """Raise ValueError unless value is non-negative, or None where that disables it."""
    if value is None:
        if not allow_none:
            raise ValueError(f"{name} must be non-negative, got None")
        return
    if value < 0:
        allowed = "non-negative or None" if allow_none else "non-negative"
        raise ValueError(f"{name} must be {allowed}, got {value}")
