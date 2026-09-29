#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

from omnimalloc.common.optional import require_optional
from omnimalloc.primitives import AllocationKind

from .model import Buffer, Model, Op

try:
    import onnx

    HAS_ONNX = True
except ImportError:
    HAS_ONNX = False
    onnx = cast("Any", None)

logger = logging.getLogger(__name__)


def _from_onnx_model(onnx_model: "onnx.ModelProto") -> Model:
    onnx.checker.check_model(onnx_model, full_check=True)
    onnx_model = onnx.shape_inference.infer_shapes(
        onnx_model,
        check_type=True,
        strict_mode=True,
        data_prop=True,
    )

    graph = onnx_model.graph
    initializers = {tensor.name for tensor in graph.initializer}
    candidates = [
        *(_tensor_proto_to_buffer(tensor) for tensor in graph.initializer),
        # Legacy IR re-lists initializers under graph inputs; those are constants.
        *(
            _value_info_to_buffer(value, AllocationKind.INPUT)
            for value in graph.input
            if value.name not in initializers
        ),
        *(_value_info_to_buffer(v, AllocationKind.OUTPUT) for v in graph.output),
        *(_value_info_to_buffer(v, AllocationKind.WORKSPACE) for v in graph.value_info),
    ]
    buffers: dict[str | int, Buffer] = {}
    for buffer in candidates:
        if buffer is None:
            continue
        if buffer.id in buffers:
            raise ValueError(f"Buffer {buffer.id} already exists")
        buffers[buffer.id] = buffer

    ops = {}
    for idx, node in enumerate(graph.node):
        # Node names are optional in ONNX; synthesize unique ids for unnamed nodes.
        op = _node_to_op(node, buffers, node.name or f"{node.op_type}_{idx}")
        if op.id in ops:
            raise ValueError(f"Node {op.id} already exists in ops")
        ops[op.id] = op

    return Model(id=graph.name or "unnamed_model", ops=ops, buffers=buffers)


def _buffer(
    name: str, dims: Iterable[int], elem_type: int, kind: AllocationKind
) -> Buffer | None:
    """The tensor's buffer, or None for a zero-size tensor, which needs no memory."""
    shape = tuple(dims)
    if 0 in shape:
        logger.debug(f"Skipping zero-size tensor '{name}' of shape {shape}")
        return None
    dtype = onnx.helper.tensor_dtype_to_np_dtype(elem_type).name
    return Buffer(id=name, shape=shape, dtype=dtype, kind=kind)


def _tensor_proto_to_buffer(tensor: "onnx.TensorProto") -> Buffer | None:
    return _buffer(tensor.name, tensor.dims, tensor.data_type, AllocationKind.CONSTANT)


def _value_info_to_buffer(
    value_info: "onnx.ValueInfoProto", kind: AllocationKind
) -> Buffer | None:
    tt = value_info.type.tensor_type
    # A symbolic dim (e.g. the batch) has no value; take it as 1
    dims = (d.dim_value if d.HasField("dim_value") else 1 for d in tt.shape.dim)
    return _buffer(value_info.name, dims, tt.elem_type, kind)


def _node_to_op(
    node: "onnx.NodeProto", buffers: dict[str | int, Buffer], op_id: str
) -> Op:
    missing = [name for name in (*node.input, *node.output) if name not in buffers]
    if missing:
        logger.debug(f"Buffers {missing} not found for node '{op_id}'")
    return Op(
        id=op_id,
        inputs={buffers[name] for name in node.input if name in buffers},
        outputs={buffers[name] for name in node.output if name in buffers},
    )


def from_onnx(onnx_input: "onnx.ModelProto | str | Path") -> Model:
    """Convert ONNX model or file path to Model."""
    if not HAS_ONNX:
        require_optional("onnx", "ONNX model conversion")

    if isinstance(onnx_input, (str, Path)):
        return _from_onnx_model(onnx.load_model(onnx_input))
    if isinstance(onnx_input, onnx.ModelProto):
        return _from_onnx_model(onnx_input)
    raise TypeError(
        f"onnx_input must be an onnx.ModelProto, str or Path, got {type(onnx_input)}"
    )
