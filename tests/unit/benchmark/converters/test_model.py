#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.benchmark.converters.model import (
    Buffer,
    Model,
    Op,
    _compute_buffer_lifetimes,
    _create_allocations,
    model_to_allocations,
    model_to_pools,
    model_to_system,
)
from omnimalloc.primitives import AllocationKind


def _buf(
    buf_id: int | str,
    kind: AllocationKind = AllocationKind.WORKSPACE,
    shape: tuple[int, ...] = (10,),
) -> Buffer:
    return Buffer(id=buf_id, shape=shape, dtype="float32", kind=kind)


def test_buffer_basic_creation_int_id() -> None:
    buffer = Buffer(
        id=0, shape=(10, 20), dtype="float32", kind=AllocationKind.WORKSPACE
    )
    assert buffer.id == 0
    assert buffer.shape == (10, 20)
    assert buffer.dtype == "float32"
    assert buffer.kind == AllocationKind.WORKSPACE


def test_buffer_basic_creation_str_id() -> None:
    buffer = Buffer(id="buf_0", shape=(10,), dtype="int8", kind=AllocationKind.CONSTANT)
    assert buffer.id == "buf_0"
    assert buffer.shape == (10,)
    assert buffer.dtype == "int8"
    assert buffer.kind == AllocationKind.CONSTANT


def test_buffer_ndim_1d() -> None:
    assert _buf(0, shape=(10,)).ndim == 1


def test_buffer_ndim_2d() -> None:
    assert _buf(0, shape=(10, 20)).ndim == 2


def test_buffer_ndim_4d() -> None:
    assert _buf(0, shape=(1, 3, 224, 224)).ndim == 4


def test_buffer_size_float32() -> None:
    buffer = Buffer(
        id=0, shape=(10, 20), dtype="float32", kind=AllocationKind.WORKSPACE
    )
    assert buffer.size == 10 * 20 * 4


def test_buffer_size_int8() -> None:
    buffer = Buffer(id=0, shape=(100,), dtype="int8", kind=AllocationKind.WORKSPACE)
    assert buffer.size == 100


def test_buffer_size_float64() -> None:
    buffer = Buffer(id=0, shape=(5, 5), dtype="float64", kind=AllocationKind.WORKSPACE)
    assert buffer.size == 5 * 5 * 8


def test_buffer_size_complex_shape() -> None:
    buffer = Buffer(
        id=0,
        shape=(2, 3, 4, 5),
        dtype="float32",
        kind=AllocationKind.WORKSPACE,
    )
    assert buffer.size == 2 * 3 * 4 * 5 * 4


def test_buffer_size_packs_sub_byte_dtypes() -> None:
    buffer = Buffer(id=0, shape=(10, 20), dtype="int4", kind=AllocationKind.WORKSPACE)
    assert buffer.size == 10 * 20 // 2


def test_buffer_size_rounds_an_odd_sub_byte_count_up() -> None:
    buffer = Buffer(id=0, shape=(7,), dtype="uint4", kind=AllocationKind.WORKSPACE)
    assert buffer.size == 4


def test_buffer_unknown_dtype() -> None:
    with pytest.raises(ValueError, match="unknown dtype 'float24'"):
        Buffer(id=0, shape=(10,), dtype="float24", kind=AllocationKind.WORKSPACE)


def test_buffer_invalid_shape_zero() -> None:
    with pytest.raises(ValueError, match="shape dimensions must be positive integers"):
        _buf(0, shape=(10, 0))


def test_buffer_invalid_shape_negative() -> None:
    with pytest.raises(ValueError, match="shape dimensions must be positive integers"):
        _buf(0, shape=(10, -5))


def test_buffer_invalid_shape_float() -> None:
    with pytest.raises(ValueError, match="shape dimensions must be positive integers"):
        _buf(0, shape=(10.5, 20))  # type: ignore[arg-type]


def test_buffer_various_kinds() -> None:
    for kind in AllocationKind:
        assert _buf(0, kind).kind == kind


def test_op_basic_creation_int_id() -> None:
    op = Op(id=0)
    assert op.id == 0
    assert op.inputs == set()
    assert op.outputs == set()
    assert op.op_type is None


def test_op_basic_creation_str_id() -> None:
    op = Op(id="conv1")
    assert op.id == "conv1"
    assert op.inputs == set()
    assert op.outputs == set()


def test_op_with_inputs_and_outputs() -> None:
    buf_in = _buf(0, AllocationKind.INPUT)
    buf_out = _buf(1, AllocationKind.OUTPUT)
    op = Op(id=0, inputs={buf_in}, outputs={buf_out})
    assert buf_in in op.inputs
    assert buf_out in op.outputs


def test_op_with_op_type() -> None:
    op = Op(id=0, op_type="Conv2D")
    assert op.op_type == "Conv2D"


def test_op_invalid_id_type() -> None:
    with pytest.raises(TypeError, match="id must be int or str"):
        Op(id=3.14)  # type: ignore[arg-type]


def test_op_duplicate_buffer_ids_input_output() -> None:
    buf = _buf(0)
    with pytest.raises(ValueError, match="buffer ids must be unique"):
        Op(id=0, inputs={buf}, outputs={buf})


def test_op_duplicate_buffer_ids_multiple_inputs() -> None:
    buf1 = _buf(0)
    buf2 = _buf(0, shape=(20,))
    with pytest.raises(ValueError, match="buffer ids must be unique"):
        Op(id=0, inputs={buf1, buf2})


def test_op_unique_buffer_ids_different_buffers() -> None:
    op = Op(id=0, inputs={_buf(0)}, outputs={_buf(1, shape=(20,))})
    assert len(op.inputs) == 1
    assert len(op.outputs) == 1


def test_op_multiple_inputs_outputs() -> None:
    op = Op(
        id=0,
        inputs={_buf(0, AllocationKind.INPUT), _buf(1, AllocationKind.INPUT)},
        outputs={_buf(2, AllocationKind.OUTPUT), _buf(3, AllocationKind.OUTPUT)},
    )
    assert len(op.inputs) == 2
    assert len(op.outputs) == 2


def test_model_basic_creation_int_id() -> None:
    model = Model(id=0)
    assert model.id == 0
    assert model.ops == {}
    assert model.buffers == {}


def test_model_basic_creation_str_id() -> None:
    model = Model(id="resnet50")
    assert model.id == "resnet50"


def test_model_with_ops_and_buffers() -> None:
    buf = _buf(0)
    op = Op(id=0, outputs={buf})
    model = Model(id=0, ops={0: op}, buffers={0: buf})
    assert 0 in model.ops
    assert 0 in model.buffers


def test_model_invalid_id_type() -> None:
    with pytest.raises(TypeError, match="id must be int or str"):
        Model(id=3.14)  # type: ignore[arg-type]


def test_model_duplicate_op_ids() -> None:
    with pytest.raises(ValueError, match="op ids must be unique"):
        Model(id=0, ops={0: Op(id=0), 1: Op(id=0)})


def test_model_duplicate_buffer_ids() -> None:
    with pytest.raises(ValueError, match="buffer ids must be unique"):
        Model(id=0, buffers={0: _buf(0), 1: _buf(0, shape=(20,))})


def test_model_unique_op_ids() -> None:
    model = Model(id=0, ops={0: Op(id=0), 1: Op(id=1)})
    assert len(model.ops) == 2


def test_model_unique_buffer_ids() -> None:
    model = Model(id=0, buffers={0: _buf(0), 1: _buf(1, shape=(20,))})
    assert len(model.buffers) == 2


def test_compute_buffer_lifetimes_basic() -> None:
    buf1 = _buf(0)
    buf2 = _buf(1)
    op1 = Op(id=0, outputs={buf1})
    op2 = Op(id=1, inputs={buf1}, outputs={buf2})
    model = Model(id=0, ops={0: op1, 1: op2}, buffers={0: buf1, 1: buf2})

    first_index, last_index = _compute_buffer_lifetimes(
        model, const_inf_lifetime=False, io_inf_lifetime=False
    )
    assert first_index[buf1] == 0
    assert last_index[buf1] == 1
    assert first_index[buf2] == 1
    assert last_index[buf2] == 1


def test_compute_buffer_lifetimes_const_inf_lifetime() -> None:
    buf_const = _buf(0, AllocationKind.CONSTANT)
    buf_work = _buf(1)
    op1 = Op(id=0, outputs={buf_const})
    op2 = Op(id=1, inputs={buf_const}, outputs={buf_work})
    model = Model(id=0, ops={0: op1, 1: op2}, buffers={0: buf_const, 1: buf_work})

    first_index, last_index = _compute_buffer_lifetimes(
        model, const_inf_lifetime=True, io_inf_lifetime=False
    )
    assert first_index[buf_const] == 0
    assert last_index[buf_const] == 1
    assert first_index[buf_work] == 1
    assert last_index[buf_work] == 1


def test_compute_buffer_lifetimes_io_inf_lifetime() -> None:
    buf_input = _buf(0, AllocationKind.INPUT)
    buf_output = _buf(1, AllocationKind.OUTPUT)
    op1 = Op(id=0, inputs={buf_input}, outputs={buf_output})
    model = Model(id=0, ops={0: op1}, buffers={0: buf_input, 1: buf_output})

    first_index, last_index = _compute_buffer_lifetimes(
        model, const_inf_lifetime=False, io_inf_lifetime=True
    )
    assert first_index[buf_input] == 0
    assert last_index[buf_input] == 0
    assert first_index[buf_output] == 0
    assert last_index[buf_output] == 0


def test_compute_buffer_lifetimes_multiple_uses() -> None:
    buf = _buf(0)
    op1 = Op(id=0, outputs={buf})
    op2 = Op(id=1, inputs={buf})
    op3 = Op(id=2, inputs={buf})
    model = Model(id=0, ops={0: op1, 1: op2, 2: op3}, buffers={0: buf})

    first_index, last_index = _compute_buffer_lifetimes(
        model, const_inf_lifetime=False, io_inf_lifetime=False
    )
    assert first_index[buf] == 0
    assert last_index[buf] == 2


def test_compute_buffer_lifetimes_all_kinds() -> None:
    buf_work = _buf(0)
    buf_const = _buf(1, AllocationKind.CONSTANT)
    buf_input = _buf(2, AllocationKind.INPUT)
    buf_output = _buf(3, AllocationKind.OUTPUT)
    op = Op(id=0, inputs={buf_input, buf_const}, outputs={buf_work, buf_output})
    model = Model(
        id=0,
        ops={0: op},
        buffers={0: buf_work, 1: buf_const, 2: buf_input, 3: buf_output},
    )

    first_index, last_index = _compute_buffer_lifetimes(
        model, const_inf_lifetime=True, io_inf_lifetime=True
    )
    for buf in (buf_work, buf_const, buf_input, buf_output):
        assert first_index[buf] == 0
        assert last_index[buf] == 0


def test_create_allocations_basic() -> None:
    buf = _buf(0)
    op = Op(id=0, outputs={buf})
    model = Model(id=0, ops={0: op}, buffers={0: buf})

    allocations = _create_allocations(
        model,
        include_const=True,
        include_io=True,
        buffer_to_first_index={buf: 0},
        buffer_to_last_index={buf: 0},
    )
    assert len(allocations) == 1
    assert allocations[0].id == 0
    assert allocations[0].size == 40
    assert allocations[0].start == 0
    assert allocations[0].end == 1
    assert allocations[0].kind == AllocationKind.WORKSPACE


def test_create_allocations_exclude_const() -> None:
    buf_work = _buf(0)
    buf_const = _buf(1, AllocationKind.CONSTANT)
    model = Model(id=0, buffers={0: buf_work, 1: buf_const})

    allocations = _create_allocations(
        model,
        include_const=False,
        include_io=True,
        buffer_to_first_index={buf_work: 0, buf_const: 0},
        buffer_to_last_index={buf_work: 0, buf_const: 0},
    )
    assert len(allocations) == 1
    assert allocations[0].kind == AllocationKind.WORKSPACE


def test_create_allocations_exclude_io() -> None:
    buf_work = _buf(0)
    buf_input = _buf(1, AllocationKind.INPUT)
    buf_output = _buf(2, AllocationKind.OUTPUT)
    model = Model(id=0, buffers={0: buf_work, 1: buf_input, 2: buf_output})
    indices = {buf_work: 0, buf_input: 0, buf_output: 0}

    allocations = _create_allocations(
        model,
        include_const=True,
        include_io=False,
        buffer_to_first_index=indices,
        buffer_to_last_index=indices,
    )
    assert len(allocations) == 1
    assert allocations[0].kind == AllocationKind.WORKSPACE


def test_create_allocations_include_all() -> None:
    buf_work = _buf(0)
    buf_const = _buf(1, AllocationKind.CONSTANT)
    buf_input = _buf(2, AllocationKind.INPUT)
    buf_output = _buf(3, AllocationKind.OUTPUT)
    model = Model(
        id=0, buffers={0: buf_work, 1: buf_const, 2: buf_input, 3: buf_output}
    )
    indices = {buf_work: 0, buf_const: 0, buf_input: 0, buf_output: 0}

    allocations = _create_allocations(
        model,
        include_const=True,
        include_io=True,
        buffer_to_first_index=indices,
        buffer_to_last_index=indices,
    )
    assert len(allocations) == 4


def test_create_allocations_exclude_all() -> None:
    buf_const = _buf(0, AllocationKind.CONSTANT)
    buf_input = _buf(1, AllocationKind.INPUT)
    model = Model(id=0, buffers={0: buf_const, 1: buf_input})
    indices = {buf_const: 0, buf_input: 0}

    allocations = _create_allocations(
        model,
        include_const=False,
        include_io=False,
        buffer_to_first_index=indices,
        buffer_to_last_index=indices,
    )
    assert len(allocations) == 0


def test_create_allocations_end_index_increment() -> None:
    buf = _buf(0)
    model = Model(id=0, buffers={0: buf})

    allocations = _create_allocations(
        model,
        include_const=True,
        include_io=True,
        buffer_to_first_index={buf: 5},
        buffer_to_last_index={buf: 10},
    )
    assert allocations[0].start == 5
    assert allocations[0].end == 11


def test_model_to_allocations_basic() -> None:
    buf = _buf(0)
    op = Op(id=0, outputs={buf})
    model = Model(id=0, ops={0: op}, buffers={0: buf})

    allocations = model_to_allocations(model)
    assert len(allocations) == 1
    assert allocations[0].size == 40


def test_model_to_allocations_exclude_const() -> None:
    buf_work = _buf(0)
    buf_const = _buf(1, AllocationKind.CONSTANT)
    op = Op(id=0, inputs={buf_const}, outputs={buf_work})
    model = Model(id=0, ops={0: op}, buffers={0: buf_work, 1: buf_const})

    allocations = model_to_allocations(model, include_const=False)
    assert len(allocations) == 1
    assert all(a.kind != AllocationKind.CONSTANT for a in allocations)


def test_model_to_allocations_include_const() -> None:
    buf_work = _buf(0)
    buf_const = _buf(1, AllocationKind.CONSTANT)
    op = Op(id=0, inputs={buf_const}, outputs={buf_work})
    model = Model(id=0, ops={0: op}, buffers={0: buf_work, 1: buf_const})

    allocations = model_to_allocations(model, include_const=True)
    assert len(allocations) == 2


def test_model_to_allocations_exclude_io() -> None:
    buf_work = _buf(0)
    buf_input = _buf(1, AllocationKind.INPUT)
    op = Op(id=0, inputs={buf_input}, outputs={buf_work})
    model = Model(id=0, ops={0: op}, buffers={0: buf_work, 1: buf_input})

    allocations = model_to_allocations(model, include_io=False)
    assert len(allocations) == 1
    assert all(not a.kind.is_io for a in allocations if a.kind)


def test_model_to_allocations_include_io() -> None:
    buf_work = _buf(0)
    buf_input = _buf(1, AllocationKind.INPUT)
    op = Op(id=0, inputs={buf_input}, outputs={buf_work})
    model = Model(id=0, ops={0: op}, buffers={0: buf_work, 1: buf_input})

    allocations = model_to_allocations(model, include_io=True)
    assert len(allocations) == 2


def test_model_to_allocations_const_inf_lifetime() -> None:
    buf_const = _buf(0, AllocationKind.CONSTANT)
    op1 = Op(id=0, outputs={buf_const})
    op2 = Op(id=1)
    op3 = Op(id=2, inputs={buf_const})
    model = Model(id=0, ops={0: op1, 1: op2, 2: op3}, buffers={0: buf_const})

    allocations = model_to_allocations(
        model, include_const=True, const_inf_lifetime=True
    )
    assert len(allocations) == 1
    assert allocations[0].start == 0
    assert allocations[0].end == 3


def test_model_to_allocations_io_inf_lifetime() -> None:
    buf_input = _buf(0, AllocationKind.INPUT)
    op1 = Op(id=0, inputs={buf_input})
    op2 = Op(id=1)
    model = Model(id=0, ops={0: op1, 1: op2}, buffers={0: buf_input})

    allocations = model_to_allocations(model, include_io=True, io_inf_lifetime=True)
    assert len(allocations) == 1
    assert allocations[0].start == 0
    assert allocations[0].end == 2


def test_model_to_allocations_complex_model() -> None:
    buf1 = _buf(0, AllocationKind.INPUT)
    buf2 = _buf(1)
    buf3 = _buf(2)
    buf4 = _buf(3, AllocationKind.OUTPUT)
    op1 = Op(id=0, inputs={buf1}, outputs={buf2})
    op2 = Op(id=1, inputs={buf2}, outputs={buf3})
    op3 = Op(id=2, inputs={buf3}, outputs={buf4})
    model = Model(
        id=0,
        ops={0: op1, 1: op2, 2: op3},
        buffers={0: buf1, 1: buf2, 2: buf3, 3: buf4},
    )

    allocations = model_to_allocations(model, include_io=True)
    assert len(allocations) == 4


def test_model_to_pools_basic() -> None:
    buf = _buf(0)
    op = Op(id=0, outputs={buf})
    model = Model(id=0, ops={0: op}, buffers={0: buf})

    pools = model_to_pools(model)
    assert len(pools) == 1
    assert pools[0].id == "workspace"


def test_model_to_pools_grouped_by_kind() -> None:
    buf_work1 = _buf(0)
    buf_work2 = _buf(1)
    buf_const = _buf(2, AllocationKind.CONSTANT)
    op = Op(id=0, inputs={buf_const}, outputs={buf_work1, buf_work2})
    model = Model(id=0, ops={0: op}, buffers={0: buf_work1, 1: buf_work2, 2: buf_const})

    pools = model_to_pools(model)
    assert len(pools) == 2
    pool_ids = {pool.id for pool in pools}
    assert "workspace" in pool_ids
    assert "constant" in pool_ids


def test_model_to_pools_all_kinds() -> None:
    buf_work = _buf(0)
    buf_const = _buf(1, AllocationKind.CONSTANT)
    buf_input = _buf(2, AllocationKind.INPUT)
    buf_output = _buf(3, AllocationKind.OUTPUT)
    op = Op(id=0, inputs={buf_input, buf_const}, outputs={buf_work, buf_output})
    model = Model(
        id=0,
        ops={0: op},
        buffers={0: buf_work, 1: buf_const, 2: buf_input, 3: buf_output},
    )

    pools = model_to_pools(model)
    assert len(pools) == 4
    assert {pool.id for pool in pools} == {"workspace", "constant", "input", "output"}


def test_model_to_pools_include_const_false() -> None:
    buf_work = _buf(0)
    buf_const = _buf(1, AllocationKind.CONSTANT)
    op = Op(id=0, inputs={buf_const}, outputs={buf_work})
    model = Model(id=0, ops={0: op}, buffers={0: buf_work, 1: buf_const})

    pools = model_to_pools(model, include_const=False)
    assert "constant" not in {pool.id for pool in pools}


def test_model_to_pools_include_io_true() -> None:
    buf_work = _buf(0)
    buf_input = _buf(1, AllocationKind.INPUT)
    op = Op(id=0, inputs={buf_input}, outputs={buf_work})
    model = Model(id=0, ops={0: op}, buffers={0: buf_work, 1: buf_input})

    pools = model_to_pools(model, include_io=True)
    assert "input" in {pool.id for pool in pools}


def test_model_to_pools_include_io_false() -> None:
    buf_work = _buf(0)
    buf_input = _buf(1, AllocationKind.INPUT)
    op = Op(id=0, inputs={buf_input}, outputs={buf_work})
    model = Model(id=0, ops={0: op}, buffers={0: buf_work, 1: buf_input})

    pools = model_to_pools(model, include_io=False)
    assert "input" not in {pool.id for pool in pools}


def test_model_to_pools_allocations_count_per_pool() -> None:
    buf_work1 = _buf(0)
    buf_work2 = _buf(1)
    buf_work3 = _buf(2)
    op = Op(id=0, outputs={buf_work1, buf_work2, buf_work3})
    model = Model(id=0, ops={0: op}, buffers={0: buf_work1, 1: buf_work2, 2: buf_work3})

    pools = model_to_pools(model)
    assert len(pools) == 1
    assert len(pools[0].allocations) == 3


def test_model_to_system_basic() -> None:
    buf = _buf(0)
    op = Op(id=0, outputs={buf})
    model = Model(id=0, ops={0: op}, buffers={0: buf})

    system = model_to_system(model)
    assert system.id == 0
    assert len(system.memories) == 1


def test_model_to_system_model_id_preserved() -> None:
    buf = _buf(0)
    op = Op(id=0, outputs={buf})
    model = Model(id="resnet50", ops={0: op}, buffers={0: buf})

    system = model_to_system(model)
    assert system.id == "resnet50"


def test_model_to_system_memory_structure() -> None:
    buf = _buf(0)
    op = Op(id=0, outputs={buf})
    model = Model(id=0, ops={0: op}, buffers={0: buf})

    system = model_to_system(model)
    assert len(system.memories) == 1
    assert system.memories[0].id == 0
    assert len(system.memories[0].pools) >= 1


def test_model_to_system_pools_included() -> None:
    buf_work = _buf(0)
    buf_const = _buf(1, AllocationKind.CONSTANT)
    buf_input = _buf(2, AllocationKind.INPUT)
    buf_output = _buf(3, AllocationKind.OUTPUT)
    op = Op(id=0, inputs={buf_input, buf_const}, outputs={buf_work, buf_output})
    model = Model(
        id=0,
        ops={0: op},
        buffers={0: buf_work, 1: buf_const, 2: buf_input, 3: buf_output},
    )

    system = model_to_system(model)
    pools = system.memories[0].pools
    assert len(pools) == 4
    assert {pool.id for pool in pools} == {"workspace", "constant", "input", "output"}


def test_model_to_system_empty_model() -> None:
    system = model_to_system(Model(id=0))
    assert len(system.memories) == 1
    assert len(system.memories[0].pools) == 0


def test_model_to_pools_skips_unreferenced_workspace_buffers() -> None:
    used = _buf("used")
    unused = _buf("unused")
    op = Op(id=0, outputs={used})
    model = Model(id=0, ops={0: op}, buffers={"used": used, "unused": unused})

    pools = model_to_pools(model)

    allocation_ids = {a.id for pool in pools for a in pool.allocations}
    assert allocation_ids == {"used"}


def test_model_to_pools_keeps_unreferenced_io_and_constant_buffers() -> None:
    used = _buf("used")
    spare_input = _buf("spare_input", AllocationKind.INPUT)
    spare_const = _buf("spare_const", AllocationKind.CONSTANT)
    op = Op(id=0, outputs={used})
    model = Model(
        id=0,
        ops={0: op},
        buffers={"used": used, "spare_input": spare_input, "spare_const": spare_const},
    )

    pools = model_to_pools(model)

    allocation_ids = {a.id for pool in pools for a in pool.allocations}
    assert allocation_ids == {"used", "spare_input", "spare_const"}


def test_model_to_allocations_skips_unreferenced_buffers() -> None:
    used = _buf("used")
    unused = _buf("unused")
    op = Op(id=0, outputs={used})
    model = Model(id=0, ops={0: op}, buffers={"used": used, "unused": unused})

    allocations = model_to_allocations(model, const_inf_lifetime=False)

    assert [a.id for a in allocations] == ["used"]


def test_model_to_allocations_handles_model_without_ops() -> None:
    buf = _buf("io", AllocationKind.INPUT)
    model = Model(id=0, buffers={"io": buf})

    allocations = model_to_allocations(model, include_io=True, io_inf_lifetime=True)

    assert len(allocations) == 1
    assert (allocations[0].start, allocations[0].end) == (0, 1)
