#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from pathlib import Path

from omnimalloc.common.optional import require_optional
from omnimalloc.primitives import BufferKind

from .model import Buffer, Model, Op

try:
    import onnx

    HAS_ONNX = True
except ImportError:
    from types import SimpleNamespace

    HAS_ONNX = False
    onnx = SimpleNamespace(  # type: ignore[assignment]
        checker=SimpleNamespace(check_model=None),
        shape_inference=SimpleNamespace(infer_shapes=None),
        load_model=None,
        helper=SimpleNamespace(tensor_dtype_to_np_dtype=None),
        ModelProto=None,
        TensorProto=None,
        ValueInfoProto=None,
        NodeProto=None,
    )

logger = logging.getLogger(__name__)


def _from_onnx_model(onnx_model: onnx.ModelProto) -> Model:
    onnx.checker.check_model(onnx_model, full_check=True)
    onnx_model = onnx.shape_inference.infer_shapes(
        onnx_model,
        check_type=True,
        strict_mode=True,
        data_prop=True,
    )

    graph = onnx_model.graph
    buffers: dict[str | int, Buffer] = {}

    def _add_buffer(buffer: Buffer) -> None:
        if buffer.id in buffers:
            raise ValueError(f"Buffer {buffer.id} already exists")
        buffers[buffer.id] = buffer

    for init in graph.initializer:
        _add_buffer(_tensor_proto_to_buffer(init))

    for inp in graph.input:
        _add_buffer(_value_info_to_buffer(inp, BufferKind.INPUT))

    for out in graph.output:
        _add_buffer(_value_info_to_buffer(out, BufferKind.OUTPUT))

    for val in graph.value_info:
        _add_buffer(_value_info_to_buffer(val, BufferKind.WORKSPACE))

    ops = {}
    for node in graph.node:
        if node.name in ops:
            raise ValueError(f"Node {node.name} already exists in ops")
        op = _node_to_op(node, buffers)
        ops[op.id] = op

    name = onnx_model.doc_string or graph.name or "unnamed_model"
    return Model(id=name, ops=ops, buffers=buffers)


def _tensor_proto_to_buffer(tensor: onnx.TensorProto) -> Buffer:
    original_shape = tuple(tensor.dims)
    shape = tuple(dim for dim in original_shape if dim > 0)
    if len(shape) != len(original_shape):
        logger.debug(
            f"Dropped dimensions with size <=0 in tensor '{tensor.name}': "
            f"{original_shape} -> {shape}"
        )
    return Buffer(
        id=tensor.name,
        shape=shape,
        dtype=onnx.helper.tensor_dtype_to_np_dtype(tensor.data_type),
        kind=BufferKind.CONSTANT,
    )


def _value_info_to_buffer(value_info: onnx.ValueInfoProto, kind: BufferKind) -> Buffer:
    tt = value_info.type.tensor_type
    original_shape = tuple(int(dim.dim_value) for dim in tt.shape.dim)
    shape = tuple(dim for dim in original_shape if dim > 0)
    if len(shape) != len(original_shape):
        logger.debug(
            f"Dropped dimensions with size <=0 in value '{value_info.name}': "
            f"{original_shape} -> {shape}"
        )
    return Buffer(
        id=value_info.name,
        shape=shape,
        dtype=onnx.helper.tensor_dtype_to_np_dtype(tt.elem_type),
        kind=kind,
    )


def _node_to_op(node: onnx.NodeProto, buffers: dict[str | int, Buffer]) -> Op:
    input_buffers = []
    for name in node.input:
        if name not in buffers:
            logger.debug(f"Input buffer '{name}' not found for node '{node.name}'")
            continue
        input_buffers.append(buffers[name])

    output_buffers = []
    for name in node.output:
        if name not in buffers:
            logger.debug(f"Output buffer '{name}' not found for node '{node.name}'")
            continue
        output_buffers.append(buffers[name])

    return Op(
        id=node.name,
        inputs=set(input_buffers),
        outputs=set(output_buffers),
        op_type=node.op_type,
    )


def from_onnx(onnx_input: onnx.ModelProto | str | Path) -> Model:
    """Convert ONNX model or file path to Model."""
    if not HAS_ONNX:
        require_optional("onnx", "ONNX model conversion")

    if isinstance(onnx_input, (str, Path)):
        return _from_onnx_model(onnx.load_model(onnx_input))
    if isinstance(onnx_input, onnx.ModelProto):
        return _from_onnx_model(onnx_input)
    raise TypeError(
        f"onnx_input must be an onnx.ModelProto or str, got {type(onnx_input)}"
    )
