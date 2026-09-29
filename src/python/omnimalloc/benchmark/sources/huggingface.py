#
# SPDX-License-Identifier: Apache-2.0
#

import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any, ClassVar, Final, cast

from omnimalloc.benchmark.converters.model import model_to_allocations
from omnimalloc.benchmark.converters.onnx import HAS_ONNX, from_onnx
from omnimalloc.common.constants import MB
from omnimalloc.common.optional import require_optional
from omnimalloc.primitives import Pool

from .base import FixedSource, prefix_ids

try:
    from huggingface_hub import HfApi, RepoFile

    HAS_HUGGINGFACE_HUB = True
except ImportError:
    HAS_HUGGINGFACE_HUB = False
    HfApi = RepoFile = cast("Any", None)

_MIN_OPSET: Final[int] = 16
_MAX_FILE_SIZE: Final[int] = 200 * MB


def _latest_opsets(repo_ids: Iterable[str]) -> list[str]:
    """Keep the highest opset of each model; the zoo has one repo per opset."""
    latest: dict[str, tuple[int, str]] = {}
    for repo_id in repo_ids:
        match = re.search(r"Opset(\d+)", repo_id)
        if match is None or int(match.group(1)) < _MIN_OPSET:
            continue
        opset = int(match.group(1))
        base_name = repo_id.replace(match.group(0), "")
        if base_name not in latest or opset > latest[base_name][0]:
            latest[base_name] = (opset, repo_id)
    return [repo_id for _, repo_id in latest.values()]


def _download_onnx_models(num_models: int, output_dir: str | Path | None) -> list[Path]:
    """Download the first `num_models` single-file ONNX zoo models of bounded size."""
    api = HfApi()
    listed = api.list_models(author="onnxmodelzoo", limit=5 * num_models)
    paths: list[Path] = []
    for repo_id in _latest_opsets(model.id for model in listed):
        if len(paths) == num_models:
            break
        files = [
            f
            for f in api.list_repo_tree(repo_id, recursive=True)
            if isinstance(f, RepoFile) and f.path.endswith(".onnx")
        ]
        if len(files) != 1 or not 0 < files[0].size <= _MAX_FILE_SIZE:
            continue
        path = api.hf_hub_download(repo_id, files[0].path, local_dir=output_dir)
        paths.append(Path(path))
    return paths


class HuggingfaceSource(FixedSource):
    """Fixed source of Huggingface ONNX model allocations, one variant per model."""

    _label_fields: ClassVar[tuple[str, ...]] = ("num_models", "output_dir")

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

    def _load_pools(self) -> tuple[Pool, ...]:
        # Models share tensor names, so their allocation ids need qualifying
        return tuple(
            prefix_ids(
                Pool(
                    id=path.stem,
                    allocations=tuple(model_to_allocations(from_onnx(path))),
                )
            )
            for path in _download_onnx_models(self.num_models, self.output_dir)
        )
