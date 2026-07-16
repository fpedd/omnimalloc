#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Final

# Cross-cutting defaults, owned here and only here: the C++ boundary (_cpp)
# takes every parameter explicitly, so a value can never drift between the
# languages. Algorithm-specific knobs live as field defaults on the
# allocator's own config dataclass instead.

# Shared wall-clock budget for every time-bounded allocator (seconds);
# None disables the budget.
DEFAULT_TIMEOUT: Final[float] = 3.0

# Shared seed for every randomized allocator and benchmark source.
DEFAULT_SEED: Final[int] = 42

# Dominance-counting budget for implicit and hot-path callers of the exact
# order queries (the omni allocator's linearize attempt, `Pool.pressure`), so
# huge vector-clock instances fail fast instead of stalling or exhausting
# memory; None means unbounded.
DEFAULT_WORK_BUDGET: Final[int] = 100_000_000

# Join-closure enumeration cap for the exact realizable-peak queries, so huge
# vector-clock instances fail fast instead of exhausting memory.
DEFAULT_CLOSURE_CAP: Final[int] = 1 << 14

# Storage units in bytes
B: Final[int] = 1
KB: Final[int] = 1_024
MB: Final[int] = 1_024 * KB
GB: Final[int] = 1_024 * MB
TB: Final[int] = 1_024 * GB

# Frequency units in hertz
HZ: Final[int] = 1
KHZ: Final[int] = 1_000 * HZ
MHZ: Final[int] = 1_000 * KHZ
GHZ: Final[int] = 1_000 * MHZ
