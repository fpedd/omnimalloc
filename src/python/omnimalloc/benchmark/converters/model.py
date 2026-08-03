#
# SPDX-License-Identifier: Apache-2.0
#

import math
from dataclasses import dataclass, field
from typing import Final

from omnimalloc.primitives import (
    Allocation,
    AllocationKind,
    IdType,
    Memory,
    Pool,
    System,
)

# Bits, not bytes: the sub-byte types are packed several to a byte, which is
# what the tensors actually occupy and what numpy's itemsize gets wrong.
ITEMBITS: Final[dict[str, int]] = {
    "int2": 2,
    "uint2": 2,
    "int4": 4,
    "uint4": 4,
    "float4_e2m1fn": 4,
    "bool": 8,
    "int8": 8,
    "uint8": 8,
    "float8_e4m3fn": 8,
    "float8_e4m3fnuz": 8,
    "float8_e5m2": 8,
    "float8_e5m2fnuz": 8,
    "float8_e8m0fnu": 8,
    "int16": 16,
    "uint16": 16,
    "float16": 16,
    "bfloat16": 16,
    "int32": 32,
    "uint32": 32,
    "float32": 32,
    "int64": 64,
    "uint64": 64,
    "float64": 64,
    "complex64": 64,
    "object": 64,
    "complex128": 128,
}


@dataclass(frozen=True)
class Buffer:
    id: IdType
    shape: tuple[int, ...]
    dtype: str
    kind: AllocationKind

    def __post_init__(self) -> None:
        if not all(isinstance(dim, int) and dim > 0 for dim in self.shape):
            raise ValueError("shape dimensions must be positive integers")
        if self.dtype not in ITEMBITS:
            raise ValueError(f"unknown dtype {self.dtype!r}")

    @property
    def ndim(self) -> int:
        return len(self.shape)

    @property
    def size(self) -> int:
        return math.ceil(ITEMBITS[self.dtype] * math.prod(self.shape) / 8)


@dataclass(frozen=True)
class Op:
    id: IdType
    inputs: set[Buffer] = field(default_factory=set)
    outputs: set[Buffer] = field(default_factory=set)
    op_type: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.id, (int, str)):
            raise TypeError(f"id must be int or str, got {type(self.id)}")
        buffer_ids = {buffer.id for buffer in self.inputs | self.outputs}
        if len(buffer_ids) != len(self.inputs) + len(self.outputs):
            raise ValueError("buffer ids must be unique across inputs and outputs")


@dataclass(frozen=True)
class Model:
    id: IdType
    ops: dict[IdType, Op] = field(default_factory=dict)
    buffers: dict[IdType, Buffer] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.id, (int, str)):
            raise TypeError(f"id must be int or str, got {type(self.id)}")
        if len(self.ops) != len({op.id for op in self.ops.values()}):
            raise ValueError("op ids must be unique")
        if len(self.buffers) != len({buffer.id for buffer in self.buffers.values()}):
            raise ValueError("buffer ids must be unique")


def _compute_buffer_lifetimes(
    model: Model,
    const_inf_lifetime: bool,
    io_inf_lifetime: bool,
) -> tuple[dict[Buffer, int], dict[Buffer, int]]:
    """Compute first and last usage indices for each buffer."""
    buffer_to_first_index: dict[Buffer, int] = {}
    buffer_to_last_index: dict[Buffer, int] = {}

    for idx, op in enumerate(model.ops.values()):
        for buffer in op.inputs | op.outputs:
            if buffer not in buffer_to_first_index:
                buffer_to_first_index[buffer] = idx
            buffer_to_last_index[buffer] = idx

    # Apply infinite lifetime constraints (at least one step for op-less models)
    max_index = max(len(model.ops) - 1, 0)
    for buffer in model.buffers.values():
        if (buffer.kind == AllocationKind.CONSTANT and const_inf_lifetime) or (
            buffer.kind.is_io and io_inf_lifetime
        ):
            buffer_to_first_index[buffer] = 0
            buffer_to_last_index[buffer] = max_index

    return buffer_to_first_index, buffer_to_last_index


def _create_allocations(
    model: Model,
    include_const: bool,
    include_io: bool,
    buffer_to_first_index: dict[Buffer, int],
    buffer_to_last_index: dict[Buffer, int],
) -> list[Allocation]:
    """Create allocations from buffers and their lifetimes."""
    return [
        Allocation(
            id=buffer.id,
            size=buffer.size,
            start=buffer_to_first_index[buffer],
            end=buffer_to_last_index[buffer] + 1,
            kind=buffer.kind,
        )
        for buffer in model.buffers.values()
        if (
            (include_const or buffer.kind != AllocationKind.CONSTANT)
            and (include_io or not buffer.kind.is_io)
            # Buffers referenced by no op have no lifetime and need no memory
            and buffer in buffer_to_first_index
        )
    ]


def model_to_allocations(
    model: Model,
    include_const: bool = False,
    include_io: bool = False,
    const_inf_lifetime: bool = True,
    io_inf_lifetime: bool = True,
) -> list[Allocation]:
    """Extract Allocations from Model buffers."""
    buffer_to_first_index, buffer_to_last_index = _compute_buffer_lifetimes(
        model, const_inf_lifetime, io_inf_lifetime
    )
    allocations = _create_allocations(
        model, include_const, include_io, buffer_to_first_index, buffer_to_last_index
    )
    return allocations


def model_to_pools(
    model: Model,
    include_const: bool = True,
    include_io: bool = True,
    const_inf_lifetime: bool = True,
    io_inf_lifetime: bool = True,
) -> tuple[Pool, ...]:
    """Extract Pools grouped by buffer kind."""
    buffer_to_first_index, buffer_to_last_index = _compute_buffer_lifetimes(
        model, const_inf_lifetime, io_inf_lifetime
    )
    allocations = _create_allocations(
        model, include_const, include_io, buffer_to_first_index, buffer_to_last_index
    )

    # Group allocations by kind
    allocations_by_kind: dict[AllocationKind, list[Allocation]] = {}
    for alloc in allocations:
        kind = alloc.kind if alloc.kind is not None else AllocationKind.WORKSPACE
        allocations_by_kind.setdefault(kind, []).append(alloc)

    return tuple(
        Pool(id=str(kind), allocations=tuple(allocs))
        for kind, allocs in allocations_by_kind.items()
    )


def model_to_system(model: Model) -> System:
    """Convert an model to a system with a single memory and pools."""
    pools = model_to_pools(model)
    memory = Memory(id=0, pools=pools)
    return System(id=model.id, memories=(memory,))
