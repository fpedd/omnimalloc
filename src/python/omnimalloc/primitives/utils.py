#
# SPDX-License-Identifier: Apache-2.0
#

import hashlib

from .allocation import Allocation, IdType


def hash_id(id_value: IdType) -> int:
    """Hash a string (or integer) ID into a stable positive integer."""

    if not isinstance(id_value, (str, int)):
        raise TypeError(f"id_value must be str or int, got {type(id_value)}")

    if isinstance(id_value, int):
        return id_value

    # Use SHA-256 for stable, deterministic hashing across Python sessions
    hash_bytes = hashlib.sha256(id_value.encode()).digest()

    # Take first 8 bytes and convert to int, then mask to positive 63-bit
    hash_val = int.from_bytes(hash_bytes[:8], "big") & 0x7FFFFFFFFFFFFFFF

    return hash_val


def get_pressure(allocations: tuple[Allocation, ...]) -> int:
    """Calculate maximum memory pressure across all allocation intervals."""
    events = [(alloc.start, alloc.size) for alloc in allocations]
    events.extend((alloc.end, -alloc.size) for alloc in allocations)
    events.sort()

    max_pressure = current = 0
    for _, delta in events:
        current += delta
        max_pressure = max(max_pressure, current)

    return max_pressure
