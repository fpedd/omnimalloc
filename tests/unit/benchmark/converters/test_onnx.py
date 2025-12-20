#
# SPDX-License-Identifier: Apache-2.0
#

import tempfile
from pathlib import Path

import numpy as np
import pytest
from omnimalloc.benchmark.converters.onnx import HAS_ONNX

if HAS_ONNX:
    import onnx
    from omnimalloc.benchmark.converters.onnx import (
        _node_to_op,
        _tensor_proto_to_buffer,
        _value_info_to_buffer,
        from_onnx,
    )
    from omnimalloc.primitives import BufferKind
    from onnx import TensorProto, helper

pytestmark = pytest.mark.skipif(not HAS_ONNX, reason="onnx not installed")


@pytest.fixture
def simple_onnx_model() -> "onnx.ModelProto":
    """Create a simple ONNX model for testing."""
    input_tensor = helper.make_tensor_value_info("input", TensorProto.FLOAT, [1, 10])
    output_tensor = helper.make_tensor_value_info("output", TensorProto.FLOAT, [1, 10])
    intermediate_tensor = helper.make_tensor_value_info(
        "intermediate", TensorProto.FLOAT, [1, 10]
    )

    rng = np.random.default_rng(42)
    weights = helper.make_tensor(
        "weights",
        TensorProto.FLOAT,
        [10, 10],
        rng.standard_normal((10, 10), dtype=np.float32).tobytes(),
        raw=True,
    )

    bias = helper.make_tensor(
        "bias",
        TensorProto.FLOAT,
        [10],
        np.zeros(10, dtype=np.float32).tobytes(),
        raw=True,
    )

    node1 = helper.make_node(
        "MatMul", ["input", "weights"], ["intermediate"], name="matmul_node"
    )
    node2 = helper.make_node(
        "Add", ["intermediate", "bias"], ["output"], name="add_node"
    )

    graph_def = helper.make_graph(
        [node1, node2],
        "test_model",
        [input_tensor],
        [output_tensor],
        [weights, bias],
        value_info=[intermediate_tensor],
    )

    model_def = helper.make_model(graph_def, producer_name="test")
    return model_def


def test_tensor_proto_to_buffer() -> None:
    """Test converting ONNX TensorProto to Buffer."""
    tensor = helper.make_tensor(
        "test_tensor",
        TensorProto.FLOAT,
        [2, 3, 4],
        np.zeros([2, 3, 4], dtype=np.float32).tobytes(),
        raw=True,
    )

    buffer = _tensor_proto_to_buffer(tensor)

    assert buffer.id == "test_tensor"
    assert buffer.shape == (2, 3, 4)
    assert buffer.dtype == np.float32
    assert buffer.kind == BufferKind.CONSTANT


def test_tensor_proto_to_buffer_different_dtype() -> None:
    """Test converting ONNX TensorProto with different data type."""
    tensor = helper.make_tensor(
        "int_tensor",
        TensorProto.INT64,
        [3, 5],
        np.ones([3, 5], dtype=np.int64).tobytes(),
        raw=True,
    )

    buffer = _tensor_proto_to_buffer(tensor)

    assert buffer.id == "int_tensor"
    assert buffer.shape == (3, 5)
    assert buffer.dtype == np.int64
    assert buffer.kind == BufferKind.CONSTANT


def test_value_info_to_buffer() -> None:
    """Test converting ONNX ValueInfoProto to Buffer."""
    value_info = helper.make_tensor_value_info("test_value", TensorProto.INT32, [5, 10])

    buffer = _value_info_to_buffer(value_info, BufferKind.WORKSPACE)

    assert buffer.id == "test_value"
    assert buffer.shape == (5, 10)
    assert buffer.dtype == np.int32
    assert buffer.kind == BufferKind.WORKSPACE


def test_value_info_to_buffer_input_kind() -> None:
    """Test converting ONNX ValueInfoProto with INPUT kind."""
    value_info = helper.make_tensor_value_info("input", TensorProto.FLOAT, [1, 3, 224])

    buffer = _value_info_to_buffer(value_info, BufferKind.INPUT)

    assert buffer.kind == BufferKind.INPUT


def test_value_info_to_buffer_output_kind() -> None:
    """Test converting ONNX ValueInfoProto with OUTPUT kind."""
    value_info = helper.make_tensor_value_info("output", TensorProto.FLOAT, [1, 1000])

    buffer = _value_info_to_buffer(value_info, BufferKind.OUTPUT)

    assert buffer.kind == BufferKind.OUTPUT


def test_value_info_to_buffer_filters_zero_dims() -> None:
    """Test that dimensions with size <= 0 are filtered."""
    value_info = helper.make_tensor_value_info(
        "test_value", TensorProto.FLOAT, [3, 0, 5]
    )

    buffer = _value_info_to_buffer(value_info, BufferKind.WORKSPACE)

    assert buffer.shape == (3, 5)


def test_node_to_op(simple_onnx_model: "onnx.ModelProto") -> None:
    """Test converting ONNX NodeProto to Op."""
    graph = simple_onnx_model.graph
    node = graph.node[0]  # MatMul node

    # Create buffer dict
    buffers = {}
    for init in graph.initializer:
        buf = _tensor_proto_to_buffer(init)
        buffers[buf.id] = buf
    for inp in graph.input:
        buf = _value_info_to_buffer(inp, BufferKind.INPUT)
        buffers[buf.id] = buf
    for val in graph.value_info:
        buf = _value_info_to_buffer(val, BufferKind.WORKSPACE)
        buffers[buf.id] = buf

    op = _node_to_op(node, buffers)

    assert op.id == "matmul_node"
    assert op.op_type == "MatMul"
    assert len(op.inputs) == 2  # input and weights
    assert len(op.outputs) == 1  # intermediate


def test_node_to_op_handles_missing_buffers(
    simple_onnx_model: "onnx.ModelProto",
) -> None:
    """Test that _node_to_op handles missing buffers gracefully."""
    graph = simple_onnx_model.graph
    node = graph.node[0]

    # Empty buffer dict
    op = _node_to_op(node, {})

    assert op.id == "matmul_node"
    assert len(op.inputs) == 0
    assert len(op.outputs) == 0


def test_from_onnx_model_proto(simple_onnx_model: "onnx.ModelProto") -> None:
    """Test converting ONNX ModelProto to Model."""
    model = from_onnx(simple_onnx_model)

    assert model.id == "test_model"
    assert len(model.ops) == 2  # MatMul and Add
    assert len(model.buffers) == 5  # input, weights, bias, intermediate, output


def test_from_onnx_file_path(simple_onnx_model: "onnx.ModelProto") -> None:
    """Test converting ONNX model from file path."""
    with tempfile.NamedTemporaryFile(suffix=".onnx", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        onnx.save(simple_onnx_model, tmp_path)
        model = from_onnx(tmp_path)

        assert model.id == "test_model"
        assert len(model.ops) == 2
        assert len(model.buffers) == 5
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def test_from_onnx_string_path(simple_onnx_model: "onnx.ModelProto") -> None:
    """Test converting ONNX model from string path."""
    with tempfile.NamedTemporaryFile(suffix=".onnx", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        onnx.save(simple_onnx_model, tmp_path)
        model = from_onnx(tmp_path)

        assert model.id == "test_model"
        assert len(model.ops) == 2
    finally:
        Path(tmp_path).unlink()


def test_from_onnx_invalid_type() -> None:
    """Test that invalid input type raises TypeError."""
    with pytest.raises(TypeError, match="onnx_input must be"):
        from_onnx(123)  # type: ignore[arg-type]


def test_from_onnx_buffer_kinds(simple_onnx_model: "onnx.ModelProto") -> None:
    """Test that buffers have correct kinds."""
    model = from_onnx(simple_onnx_model)

    input_buffer = model.buffers["input"]
    assert input_buffer.kind == BufferKind.INPUT

    output_buffer = model.buffers["output"]
    assert output_buffer.kind == BufferKind.OUTPUT

    weights_buffer = model.buffers["weights"]
    assert weights_buffer.kind == BufferKind.CONSTANT

    bias_buffer = model.buffers["bias"]
    assert bias_buffer.kind == BufferKind.CONSTANT

    intermediate_buffer = model.buffers["intermediate"]
    assert intermediate_buffer.kind == BufferKind.WORKSPACE


def test_from_onnx_ops_reference_buffers(simple_onnx_model: "onnx.ModelProto") -> None:
    """Test that ops correctly reference buffers."""
    model = from_onnx(simple_onnx_model)

    matmul_op = model.ops["matmul_node"]
    input_ids = {buf.id for buf in matmul_op.inputs}
    assert "input" in input_ids
    assert "weights" in input_ids

    output_ids = {buf.id for buf in matmul_op.outputs}
    assert "intermediate" in output_ids

    add_op = model.ops["add_node"]
    add_input_ids = {buf.id for buf in add_op.inputs}
    assert "intermediate" in add_input_ids
    assert "bias" in add_input_ids

    add_output_ids = {buf.id for buf in add_op.outputs}
    assert "output" in add_output_ids
