#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import Allocation, AllocationKind

# Type alias for allocation identifiers (int or str)
# Must match IdType in src/cpp/primitives/id_type.hpp
IdType = int | str

# Type alias for a vector-clock value with one component per thread; also the
# type of a cut of the execution (any componentwise-max join of clock values)
# Must match the vector alternative of TimePoint in src/cpp/primitives/allocation.hpp
VectorClock = tuple[int, ...]

# Type alias for lifetime bounds: a scalar step on one global timeline, or a
# vector clock (1-tuples normalize to scalars)
# Must match TimePoint in src/cpp/primitives/allocation.hpp
TimePoint = int | VectorClock

__all__ = ["Allocation", "AllocationKind", "IdType", "TimePoint", "VectorClock"]
