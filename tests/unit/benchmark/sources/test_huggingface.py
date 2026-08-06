#
# SPDX-License-Identifier: Apache-2.0
#

import functools
from collections.abc import Callable
from pathlib import Path

import pytest
from omnimalloc.benchmark.sources.huggingface import (
    HAS_HUGGINGFACE_HUB,
    HAS_ONNX,
    HuggingfaceSource,
)

pytestmark = pytest.mark.skipif(
    not (HAS_ONNX and HAS_HUGGINGFACE_HUB),
    reason="onnx and huggingface-hub not installed",
)


def skip_when_rate_limited(test: Callable[..., None]) -> Callable[..., None]:
    @functools.wraps(test)
    def wrapper(*args: object, **kwargs: object) -> None:
        # Local import so collection works without huggingface_hub installed
        from huggingface_hub.errors import HfHubHTTPError

        try:
            test(*args, **kwargs)
        except HfHubHTTPError as error:
            if error.response.status_code == 429:
                pytest.skip("Hugging Face Hub rate limited the request")
            raise

    return wrapper


def test_huggingface_source_creation() -> None:
    """Test basic HuggingfaceSource instantiation."""
    source = HuggingfaceSource(num_models=1)
    assert source.num_models == 1
    assert source._model_paths is None  # noqa: SLF001
    assert source._model_pools is None  # noqa: SLF001
    # HuggingfaceSource is a fixed source, so num_allocations is from base class
    assert source.num_allocations == 100  # Default from BaseSource


def test_huggingface_source_creation_with_params(artifacts_dir: Path) -> None:
    """Test HuggingfaceSource with custom parameters."""
    source = HuggingfaceSource(num_models=2, output_dir=str(artifacts_dir))
    assert source.num_models == 2
    assert source.output_dir == str(artifacts_dir)


@skip_when_rate_limited
def test_huggingface_source_get_allocations_single_model(artifacts_dir: Path) -> None:
    """Test downloading and extracting allocations from a single model.

    This test downloads from Huggingface and may be slow.
    """
    source = HuggingfaceSource(num_models=1, output_dir=str(artifacts_dir))
    allocations = source.get_allocations()

    # Should have allocations from the model
    assert len(allocations) > 0
    assert all(hasattr(alloc, "id") for alloc in allocations)
    assert all(hasattr(alloc, "size") for alloc in allocations)
    assert all(hasattr(alloc, "start") for alloc in allocations)
    assert all(hasattr(alloc, "end") for alloc in allocations)


@skip_when_rate_limited
def test_huggingface_source_get_allocations_with_count(artifacts_dir: Path) -> None:
    """Test getting a limited number of allocations."""
    source = HuggingfaceSource(num_models=1, output_dir=str(artifacts_dir))
    allocations = source.get_allocations(num_allocations=5)

    # Should have at most 5 allocations
    assert len(allocations) <= 5


@skip_when_rate_limited
def test_huggingface_source_get_allocations_with_skip(artifacts_dir: Path) -> None:
    """Test skipping allocations."""
    source = HuggingfaceSource(num_models=1, output_dir=str(artifacts_dir))
    all_allocations = source.get_allocations()
    skipped_allocations = source.get_allocations(skip=2)

    # Should skip first 2
    if len(all_allocations) > 2:
        assert len(skipped_allocations) == len(all_allocations) - 2
        assert skipped_allocations[0] == all_allocations[2]


@skip_when_rate_limited
def test_huggingface_source_caching(artifacts_dir: Path) -> None:
    """Test that models are cached after first download."""
    source = HuggingfaceSource(num_models=1, output_dir=str(artifacts_dir))

    # First call downloads
    allocations1 = source.get_allocations()
    assert source._model_paths is not None  # noqa: SLF001
    assert source._model_pools is not None  # noqa: SLF001

    # Second call uses cache
    allocations2 = source.get_allocations()
    assert allocations1 == allocations2


@skip_when_rate_limited
def test_huggingface_source_allocation_kinds(artifacts_dir: Path) -> None:
    """Test that allocations have appropriate allocation kinds."""
    source = HuggingfaceSource(num_models=1, output_dir=str(artifacts_dir))
    allocations = source.get_allocations()

    # Should have various allocation kinds
    kinds = {alloc.kind for alloc in allocations if alloc.kind is not None}
    # At least workspace or input/output/constant should be present
    assert len(kinds) > 0
