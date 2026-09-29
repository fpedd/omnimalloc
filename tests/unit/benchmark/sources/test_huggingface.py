#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc.benchmark.sources import huggingface
from omnimalloc.benchmark.sources.huggingface import (
    HAS_HUGGINGFACE_HUB,
    HAS_ONNX,
    HuggingfaceSource,
)

pytestmark = pytest.mark.skipif(
    not (HAS_ONNX and HAS_HUGGINGFACE_HUB),
    reason="onnx and huggingface-hub not installed",
)


def _save_tiny_model(path: Path) -> Path:
    """Input -> Relu -> hidden -> Relu -> output, one workspace tensor."""
    import onnx
    from onnx import TensorProto, helper

    def value(name: str) -> "onnx.ValueInfoProto":
        return helper.make_tensor_value_info(name, TensorProto.FLOAT, [1, 8])

    graph = helper.make_graph(
        [
            helper.make_node("Relu", ["input"], ["hidden"], name="relu_0"),
            helper.make_node("Relu", ["hidden"], ["output"], name="relu_1"),
        ],
        path.stem,
        [value("input")],
        [value("output")],
        value_info=[value("hidden")],
    )
    onnx.save(helper.make_model(graph), path)
    return path


@pytest.fixture
def local_models(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve two tiny local models in place of the Hub download."""
    paths = [_save_tiny_model(tmp_path / f"model_{i}.onnx") for i in range(2)]
    monkeypatch.setattr(
        huggingface, "_download_onnx_models", lambda num_models, _: paths[:num_models]
    )


@pytest.mark.usefixtures("local_models")
def test_huggingface_source_downloads_lazily() -> None:
    source = HuggingfaceSource(num_models=2)
    assert "pools" not in vars(source)
    assert source.get_available_variants() == ("model_0", "model_1")


@pytest.mark.usefixtures("local_models")
def test_huggingface_source_qualifies_ids_across_models() -> None:
    allocations = HuggingfaceSource(num_models=2).get_allocations()
    assert [a.id for a in allocations] == ["model_0_hidden", "model_1_hidden"]


@pytest.mark.usefixtures("local_models")
def test_huggingface_source_get_variant_by_name_and_index() -> None:
    source = HuggingfaceSource(num_models=2)
    assert source.get_variant("model_1") is source.get_variant(1)
    with pytest.raises(ValueError, match="not found"):
        source.get_variant("model_2")


@pytest.mark.usefixtures("local_models")
def test_huggingface_source_get_allocations_slices() -> None:
    source = HuggingfaceSource(num_models=2)
    assert source.get_allocations(num_allocations=1, skip=1) == (
        source.get_allocations()[1],
    )


def test_huggingface_source_downloads_a_real_model(tmp_path: Path) -> None:
    from huggingface_hub.errors import HfHubHTTPError

    source = HuggingfaceSource(num_models=1, output_dir=tmp_path)
    try:
        pools = source.pools
    except HfHubHTTPError as error:
        if error.response.status_code == 429:
            pytest.skip("Hugging Face Hub rate limited the request")
        raise
    assert len(pools) == 1
    assert pools[0].allocations
