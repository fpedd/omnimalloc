#
# SPDX-License-Identifier: Apache-2.0
#

import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from omnimalloc.benchmark.converters.model import model_to_allocations
from omnimalloc.benchmark.converters.onnx import from_onnx
from omnimalloc.common.optional import require_optional
from omnimalloc.primitives import Allocation, IdType, Pool

from .base import BaseSource

try:
    import onnx

    HAS_ONNX = True
except ImportError:
    from types import SimpleNamespace

    HAS_ONNX = False
    onnx = SimpleNamespace(  # type: ignore[assignment]
        load=None,
    )

try:
    from huggingface_hub import HfApi, ModelInfo

    HAS_HUGGINGFACE_HUB = True
except ImportError:
    HAS_HUGGINGFACE_HUB = False
    HfApi = None  # type: ignore[assignment,misc]
    ModelInfo = None  # type: ignore[assignment,misc]

try:
    from tqdm.auto import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

    # Fallback: tqdm without progress bars (just returns iterable)
    def tqdm(iterable: Any, **kwargs: Any) -> Any:  # noqa: ARG001, ANN401
        """No-op tqdm fallback when tqdm is not installed."""
        return iterable


logger = logging.getLogger(__name__)


def _get_hf_api() -> HfApi:
    """Get HfApi instance, checking that dependency is available."""
    if not HAS_HUGGINGFACE_HUB:
        require_optional("huggingface-hub", "HuggingfaceSource")
    return HfApi()


def _list_onnx_models(limit: int = 10, search: str | None = None) -> list[ModelInfo]:
    """Return ONNX models from Hugging Face Hub, excluding the first result."""
    hf_api = _get_hf_api()
    models = hf_api.list_models(
        author="onnxmodelzoo",
        search=search,
        limit=limit + 1,
        filter=None,
    )
    return list(models)[1:]  # Exclude first, which is a legacy model repository


def _filter_onnx_opsets(
    model_infos: list[ModelInfo], min_opset: int = 16
) -> list[ModelInfo]:
    """Filter ONNX models to only include the highest opset per base model name."""
    model_groups = defaultdict(list)

    for model_info in model_infos:
        match = re.search(r"Opset(\d+)", model_info.id)
        if not match:
            continue
        opset = int(match.group(1))
        if opset < min_opset:
            continue
        base_name = re.sub(r"Opset\d+", "", model_info.id)
        model_groups[base_name].append((opset, model_info))

    return [max(models, key=lambda x: x[0])[1] for models in model_groups.values()]


def _gather_download_info(
    model_infos: list[ModelInfo],
    filename_filter: str,
    max_file_size_mb: float | None = 200,
) -> dict[str, str]:
    """Gather information about which models to download, filtering by size."""
    hf_api = _get_hf_api()
    id_file_map = {}

    for model_info in model_infos:
        repo_files = hf_api.list_repo_tree(model_info.id, recursive=True)
        onnx_files = [
            f
            for f in repo_files
            if f.path.endswith(filename_filter) and hasattr(f, "size")
        ]

        if len(onnx_files) != 1:
            continue

        file_info = onnx_files[0]
        if file_info.size is None:
            continue

        # At this point, file_info.size is guaranteed to not be None
        file_size = file_info.size
        assert file_size is not None
        # ty doesn't respect the assert for narrowing
        file_size_mb = file_size / (1024 * 1024)  # type: ignore[unsupported-operator]
        if max_file_size_mb is not None and file_size_mb > max_file_size_mb:
            continue

        id_file_map[model_info.id] = file_info.path

    return id_file_map


def _download_files(
    id_file_map: dict[str, str],
    output_dir: str | Path | None = None,
    filename_filter: str = ".onnx",
) -> list[Path]:
    """Download files from Hugging Face Hub and return their local paths."""
    hf_api = _get_hf_api()
    desc = f"Downloading {len(id_file_map)} '{filename_filter}' models from HuggingFace"

    local_paths = []
    for repo_id, filename in tqdm(id_file_map.items(), desc=desc, leave=False):
        local_path = hf_api.hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            local_dir=output_dir,
        )
        local_paths.append(Path(local_path))

    return local_paths


# TODO(fpedd): This is really slow, don't do this on the files,
# but on the onnx models directly
def _validate_onnx_files(file_paths: list[Path]) -> None:
    """Validate and canonicalize ONNX files in place."""
    desc = f"Validating and canonicalizing {len(file_paths)} ONNX models"
    for file_path in tqdm(file_paths, desc=desc, leave=False):
        onnx_model = onnx.load_model(file_path)
        onnx.checker.check_model(onnx_model, full_check=True)
        onnx_model = onnx.shape_inference.infer_shapes(
            onnx_model,
            check_type=True,
            strict_mode=True,
            data_prop=True,
        )
        onnx_model.doc_string = str(file_path.stem)
        onnx.save_model(onnx_model, file_path)


def _download_onnx_models(
    num_models: int = 10, output_dir: str | Path | None = None
) -> list[Path]:
    """Download ONNX models and return local file paths."""
    models = _list_onnx_models(limit=num_models * 5)
    filtered = _filter_onnx_opsets(models)
    id_file_map = _gather_download_info(filtered, ".onnx", max_file_size_mb=200)
    id_file_map_limited = dict(list(id_file_map.items())[:num_models])
    paths = _download_files(id_file_map_limited, output_dir, ".onnx")
    _validate_onnx_files(paths)
    return paths


class HuggingfaceSource(BaseSource):
    """Load allocations from Huggingface ONNX models.

    This is a fixed source with predetermined models from Huggingface.
    Each model becomes a variant that can be benchmarked.
    """

    def __init__(
        self,
        num_models: int = 1,
        output_dir: str | Path | None = None,
    ) -> None:
        if not HAS_ONNX:
            require_optional("onnx", "HuggingfaceSource")
        if not HAS_HUGGINGFACE_HUB:
            require_optional("huggingface-hub", "HuggingfaceSource")

        super().__init__()
        self.num_models = num_models
        self.output_dir = output_dir

        self._model_paths: list[Path] | None = None
        self._model_pools: dict[str, Pool] | None = None

    def _ensure_downloaded(self) -> None:
        if self._model_paths is not None and self._model_pools is not None:
            return

        self._model_paths = _download_onnx_models(self.num_models, self.output_dir)
        self._model_pools = {}
        for model_path in self._model_paths:
            model = from_onnx(model_path)
            allocations = model_to_allocations(model)
            model_name = model_path.stem
            pool = Pool(id=f"hf_{model_name}", allocations=tuple(allocations))
            self._model_pools[model_name] = pool

    def is_parameterizable(self) -> bool:
        return False

    def get_available_variants(self, variants: int | None = None) -> tuple[str, ...]:
        if variants is not None:
            self.num_models = max(self.num_models, variants)
        self._ensure_downloaded()
        assert self._model_pools is not None
        return tuple(self._model_pools.keys())

    def get_variant(self, variant_id: IdType) -> Pool:
        self._ensure_downloaded()
        assert self._model_pools is not None

        if isinstance(variant_id, int):
            model_names = list(self._model_pools.keys())
            if not (0 <= variant_id < len(model_names)):
                msg = f"Model index {variant_id} out of range [0, {len(model_names)})"
                raise ValueError(msg)
            return self._model_pools[model_names[variant_id]]

        if variant_id not in self._model_pools:
            msg = f"Model '{variant_id}' not found in Huggingface source"
            raise ValueError(msg)

        return self._model_pools[variant_id]

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        self._ensure_downloaded()
        assert self._model_pools is not None

        all_allocations: list[Allocation] = []
        for pool in self._model_pools.values():
            all_allocations.extend(pool.allocations)

        if skip >= len(all_allocations):
            return ()
        end = (
            len(all_allocations) if num_allocations is None else skip + num_allocations
        )
        return tuple(all_allocations[skip:end])
