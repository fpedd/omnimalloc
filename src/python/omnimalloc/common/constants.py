#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Final

# Shared wall-clock budget for every time-bounded allocator (seconds);
# None disables the budget.
DEFAULT_TIMEOUT: Final[float] = 3.0

# Shared seed for every randomized allocator and benchmark source.
DEFAULT_SEED: Final[int] = 42

# Work budget for the exact order queries and the conflict sweep, counted in
# elementary clock-component comparisons, so that huge vector-clock instances
# fail fast instead of stalling or exhausting memory; None means unbounded.
DEFAULT_WORK_BUDGET: Final[int] = 10_000_000_000

# Default budget for the entry points that materialize a structure per scanned
# unit (`conflicts`' id-keyed map, `antichain_pressure`'s flow arcs): ~100x the
# cost per unit, so a 100x tighter default keeps the wall-clock cap comparable.
DEFAULT_MATERIALIZE_BUDGET: Final[int] = DEFAULT_WORK_BUDGET // 100

# `conflicts` is the one entry point whose output, not its work, is the binding
# resource: ~260 bytes per pair as Python sets against 8 in the CSR. Sized to
# refuse near-complete relations, so a memory-limited caller passes its own.
DEFAULT_CONFLICT_MAP_BUDGET: Final[int] = 10_000_000

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
