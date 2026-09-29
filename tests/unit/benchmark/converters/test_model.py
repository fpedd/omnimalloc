#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.benchmark.converters.model import (
    Buffer,
    Model,
    Op,
    model_to_allocations,
)
from omnimalloc.primitives import AllocationKind

WORKSPACE = AllocationKind.WORKSPACE
CONSTANT = AllocationKind.CONSTANT
INPUT = AllocationKind.INPUT
OUTPUT = AllocationKind.OUTPUT


def _buf(
    buf_id: int | str,
    kind: AllocationKind = WORKSPACE,
    shape: tuple[int, ...] = (10,),
    dtype: str = "float32",
) -> Buffer:
    return Buffer(id=buf_id, shape=shape, dtype=dtype, kind=kind)


@pytest.mark.parametrize(
    ("shape", "dtype", "size"),
    [
        ((10, 20), "float32", 800),
        ((100,), "int8", 100),
        ((5, 5), "float64", 200),
        ((2, 3, 4, 5), "float32", 480),
        ((10, 20), "int4", 100),
        ((7,), "uint4", 4),
    ],
)
def test_buffer_size(shape: tuple[int, ...], dtype: str, size: int) -> None:
    assert _buf(0, shape=shape, dtype=dtype).size == size


def test_buffer_rejects_unknown_dtype() -> None:
    with pytest.raises(ValueError, match="unknown dtype 'float24'"):
        _buf(0, dtype="float24")


@pytest.mark.parametrize("shape", [(10, 0), (10, -5), (10.5, 20)])
def test_buffer_rejects_non_positive_integer_shapes(shape: tuple[int, ...]) -> None:
    with pytest.raises(ValueError, match="shape dimensions must be positive integers"):
        _buf(0, shape=shape)


def test_op_rejects_invalid_id_type() -> None:
    with pytest.raises(TypeError, match="id must be int or str"):
        Op(id=3.14)  # type: ignore[arg-type]


def test_op_rejects_a_buffer_id_used_twice() -> None:
    with pytest.raises(ValueError, match="buffer ids must be unique"):
        Op(id=0, inputs={_buf(0)}, outputs={_buf(0)})
    with pytest.raises(ValueError, match="buffer ids must be unique"):
        Op(id=0, inputs={_buf(0), _buf(0, shape=(20,))})


def test_model_rejects_invalid_id_type() -> None:
    with pytest.raises(TypeError, match="id must be int or str"):
        Model(id=3.14)  # type: ignore[arg-type]


def test_model_rejects_duplicate_ids() -> None:
    with pytest.raises(ValueError, match="op ids must be unique"):
        Model(id=0, ops={0: Op(id=0), 1: Op(id=0)})
    with pytest.raises(ValueError, match="buffer ids must be unique"):
        Model(id=0, buffers={0: _buf(0), 1: _buf(0, shape=(20,))})


def _chain() -> Model:
    """Input -> a -> b -> output through three ops, reading one constant."""
    bufs = {
        "input": _buf("input", INPUT),
        "const": _buf("const", CONSTANT),
        "a": _buf("a"),
        "b": _buf("b"),
        "output": _buf("output", OUTPUT),
    }
    ops = {
        0: Op(id=0, inputs={bufs["input"]}, outputs={bufs["a"]}),
        1: Op(id=1, inputs={bufs["a"]}, outputs={bufs["b"]}),
        2: Op(id=2, inputs={bufs["b"], bufs["const"]}, outputs={bufs["output"]}),
    }
    return Model(id="chain", ops=ops, buffers=bufs)


def _lifetimes(**kwargs: bool) -> dict[object, tuple[object, object]]:
    return {a.id: (a.start, a.end) for a in model_to_allocations(_chain(), **kwargs)}


def test_model_to_allocations_keeps_workspace_by_default() -> None:
    assert _lifetimes() == {"a": (0, 2), "b": (1, 3)}


def test_model_to_allocations_spans_constants_and_io_over_the_model() -> None:
    lifetimes = _lifetimes(include_const=True, include_io=True)
    assert lifetimes["const"] == lifetimes["input"] == lifetimes["output"] == (0, 3)


@pytest.mark.parametrize(
    ("kwargs", "extra"),
    [
        ({"include_const": True}, {"const"}),
        ({"include_io": True}, {"input", "output"}),
    ],
)
def test_model_to_allocations_includes_by_kind(
    kwargs: dict[str, bool], extra: set[str]
) -> None:
    assert set(_lifetimes(**kwargs)) == {"a", "b"} | extra


def test_model_to_allocations_skips_unreferenced_buffers() -> None:
    used, unused = _buf("used"), _buf("unused")
    model = Model(
        id=0,
        ops={0: Op(id=0, outputs={used})},
        buffers={"used": used, "unused": unused},
    )
    assert [a.id for a in model_to_allocations(model)] == ["used"]


def test_model_to_allocations_handles_model_without_ops() -> None:
    model = Model(id=0, buffers={"io": _buf("io", INPUT)})
    allocations = model_to_allocations(model, include_io=True)
    assert [(a.start, a.end) for a in allocations] == [(0, 1)]
