#
# SPDX-License-Identifier: Apache-2.0
#

import math
from pathlib import Path

import pytest
from omnimalloc.benchmark.converters.onnx import HAS_ONNX

if HAS_ONNX:
    import numpy as np
    import onnx
    from omnimalloc.benchmark.converters.model import ITEMBITS
    from omnimalloc.benchmark.converters.onnx import (
        _node_to_op,
        _tensor_proto_to_buffer,
        _value_info_to_buffer,
        from_onnx,
    )
    from omnimalloc.primitives import AllocationKind
    from onnx import TensorProto, helper

pytestmark = pytest.mark.skipif(not HAS_ONNX, reason="onnx not installed")


@pytest.fixture
def simple_onnx_model() -> "onnx.ModelProto":
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

    return helper.make_model(graph_def, producer_name="test")


# The widths onnx.helper.make_tensor packs a raw tensor to, for the types numpy
# has no storage for; every other type is one numpy itemsize per element.
SUB_BYTE_BITS = {"INT2": 2, "UINT2": 2, "INT4": 4, "UINT4": 4, "FLOAT4E2M1": 4}


def test_itembits_covers_every_onnx_dtype() -> None:
    """ITEMBITS must stay in step with the dtypes ONNX can hand the converter."""
    for name, value in TensorProto.DataType.items():
        if value == TensorProto.UNDEFINED:
            continue
        dtype = onnx.helper.tensor_dtype_to_np_dtype(value)
        expected = SUB_BYTE_BITS.get(name, dtype.itemsize * 8)
        assert ITEMBITS.get(dtype.name) == expected, name


@pytest.mark.parametrize(
    ("dtype_name", "count"), [("INT4", 8), ("INT4", 7), ("UINT4", 3)]
)
def test_sub_byte_buffers_are_sized_packed(dtype_name: str, count: int) -> None:
    """A 4-bit tensor occupies half a byte per element, not numpy's full one."""
    value = TensorProto.DataType.Value(dtype_name)
    packed = helper.make_tensor(
        dtype_name, value, [count], b"\x00" * math.ceil(count / 2), raw=True
    )

    assert _tensor_proto_to_buffer(packed).size == len(packed.raw_data)


def test_tensor_proto_to_buffer() -> None:
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
    assert buffer.dtype == "float32"
    assert buffer.kind == AllocationKind.CONSTANT


def test_tensor_proto_to_buffer_different_dtype() -> None:
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
    assert buffer.dtype == "int64"
    assert buffer.kind == AllocationKind.CONSTANT


def test_value_info_to_buffer() -> None:
    value_info = helper.make_tensor_value_info("test_value", TensorProto.INT32, [5, 10])

    buffer = _value_info_to_buffer(value_info, AllocationKind.WORKSPACE)

    assert buffer.id == "test_value"
    assert buffer.shape == (5, 10)
    assert buffer.dtype == "int32"
    assert buffer.kind == AllocationKind.WORKSPACE


def test_value_info_to_buffer_input_kind() -> None:
    value_info = helper.make_tensor_value_info("input", TensorProto.FLOAT, [1, 3, 224])

    buffer = _value_info_to_buffer(value_info, AllocationKind.INPUT)

    assert buffer.kind == AllocationKind.INPUT


def test_value_info_to_buffer_output_kind() -> None:
    value_info = helper.make_tensor_value_info("output", TensorProto.FLOAT, [1, 1000])

    buffer = _value_info_to_buffer(value_info, AllocationKind.OUTPUT)

    assert buffer.kind == AllocationKind.OUTPUT


def test_value_info_to_buffer_filters_zero_dims() -> None:
    value_info = helper.make_tensor_value_info(
        "test_value", TensorProto.FLOAT, [3, 0, 5]
    )

    buffer = _value_info_to_buffer(value_info, AllocationKind.WORKSPACE)

    assert buffer.shape == (3, 5)


def test_node_to_op(simple_onnx_model: "onnx.ModelProto") -> None:
    graph = simple_onnx_model.graph
    node = graph.node[0]

    buffers = {}
    for init in graph.initializer:
        buf = _tensor_proto_to_buffer(init)
        buffers[buf.id] = buf
    for inp in graph.input:
        buf = _value_info_to_buffer(inp, AllocationKind.INPUT)
        buffers[buf.id] = buf
    for val in graph.value_info:
        buf = _value_info_to_buffer(val, AllocationKind.WORKSPACE)
        buffers[buf.id] = buf

    op = _node_to_op(node, buffers, node.name)

    assert op.id == "matmul_node"
    assert op.op_type == "MatMul"
    assert len(op.inputs) == 2
    assert len(op.outputs) == 1


def test_node_to_op_handles_missing_buffers(
    simple_onnx_model: "onnx.ModelProto",
) -> None:
    node = simple_onnx_model.graph.node[0]

    op = _node_to_op(node, {}, node.name)

    assert op.id == "matmul_node"
    assert len(op.inputs) == 0
    assert len(op.outputs) == 0


def test_from_onnx_model_proto(simple_onnx_model: "onnx.ModelProto") -> None:
    model = from_onnx(simple_onnx_model)

    assert model.id == "test_model"
    assert len(model.ops) == 2
    assert len(model.buffers) == 5


@pytest.mark.parametrize("path_type", [Path, str])
def test_from_onnx_path(
    simple_onnx_model: "onnx.ModelProto", tmp_path: Path, path_type: type
) -> None:
    model_path = tmp_path / "model.onnx"
    onnx.save(simple_onnx_model, model_path)

    model = from_onnx(path_type(model_path))

    assert model.id == "test_model"
    assert len(model.ops) == 2
    assert len(model.buffers) == 5


def test_from_onnx_invalid_type() -> None:
    with pytest.raises(TypeError, match="onnx_input must be"):
        from_onnx(123)  # type: ignore[arg-type]


def test_from_onnx_buffer_kinds(simple_onnx_model: "onnx.ModelProto") -> None:
    model = from_onnx(simple_onnx_model)

    assert model.buffers["input"].kind == AllocationKind.INPUT
    assert model.buffers["output"].kind == AllocationKind.OUTPUT
    assert model.buffers["weights"].kind == AllocationKind.CONSTANT
    assert model.buffers["bias"].kind == AllocationKind.CONSTANT
    assert model.buffers["intermediate"].kind == AllocationKind.WORKSPACE


def test_from_onnx_ops_reference_buffers(simple_onnx_model: "onnx.ModelProto") -> None:
    model = from_onnx(simple_onnx_model)

    matmul_op = model.ops["matmul_node"]
    assert {buf.id for buf in matmul_op.inputs} == {"input", "weights"}
    assert {buf.id for buf in matmul_op.outputs} == {"intermediate"}

    add_op = model.ops["add_node"]
    assert {buf.id for buf in add_op.inputs} == {"intermediate", "bias"}
    assert {buf.id for buf in add_op.outputs} == {"output"}


def test_from_onnx_synthesizes_ids_for_unnamed_nodes(
    simple_onnx_model: "onnx.ModelProto",
) -> None:
    for node in simple_onnx_model.graph.node:
        node.name = ""

    model = from_onnx(simple_onnx_model)

    assert set(model.ops) == {"MatMul_0", "Add_1"}


def test_from_onnx_skips_initializers_relisted_as_inputs(
    simple_onnx_model: "onnx.ModelProto",
) -> None:
    weights = next(
        i for i in simple_onnx_model.graph.initializer if i.name == "weights"
    )
    value_info = helper.make_tensor_value_info(
        "weights", TensorProto.FLOAT, list(weights.dims)
    )
    simple_onnx_model.graph.input.append(value_info)

    model = from_onnx(simple_onnx_model)

    assert model.buffers["weights"].kind == AllocationKind.CONSTANT
