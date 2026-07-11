#
# SPDX-License-Identifier: Apache-2.0
#

import time
from pathlib import Path
from typing import Any, Final, cast

import numpy as np

from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.common.optional import require_optional
from omnimalloc.primitives import Allocation

from .base import DEFAULT_MAX_SECONDS, BaseAllocator, require_unique_ids

try:
    import torch

    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    torch = cast("Any", None)

# Weights shipped with the package, produced by training/train_neural.py
DEFAULT_WEIGHTS: Final[Path] = Path(__file__).with_name("neural_weights.pt")


def peak_lower_bound(allocations: tuple[Allocation, ...]) -> int:
    """Maximum live load over time: a lower bound on any peak memory."""
    events = sorted(
        [(a.start, a.size) for a in allocations]
        + [(a.end, -a.size) for a in allocations]
    )
    load = peak = 0
    for _, delta in events:
        load += delta
        peak = max(peak, load)
    return peak


def classic_orders(allocations: tuple[Allocation, ...]) -> list[list[int]]:
    """Index permutations of the classic greedy orderings (identity last)."""
    from .greedy_base import (
        order_by_area,
        order_by_conflict,
        order_by_conflict_size,
        order_by_duration,
        order_by_size,
        order_by_start,
    )

    index = {a.id: i for i, a in enumerate(allocations)}
    orderings = (
        order_by_size,
        order_by_duration,
        order_by_area,
        order_by_conflict,
        order_by_conflict_size,
        order_by_start,
    )
    orders = [[index[a.id] for a in ordering(allocations)] for ordering in orderings]
    orders.append(list(range(len(allocations))))
    return orders


def extract_features(
    allocations: tuple[Allocation, ...],
    placer: FirstFitPlacer,
) -> np.ndarray:
    """Per-allocation feature matrix (n, FEATURE_DIM) for the priority model.

    Scale-invariant: sizes, times, and areas are normalized per instance, and
    rank features expose the orderings the classic greedy heuristics use.
    Instance-level context (the relative peak of every classic greedy order
    and a one-hot of the winner) is broadcast to every allocation so the model
    can condition on which heuristic family suits the instance.
    """
    n = len(allocations)
    overlaps = placer.overlaps
    size = np.array([a.size for a in allocations], dtype=np.float64)
    start = np.array([a.start for a in allocations], dtype=np.float64)
    end = np.array([a.end for a in allocations], dtype=np.float64)
    duration = end - start
    area = size * duration

    span = max(end.max() - start.min(), 1.0)
    t0 = start.min()

    size_by_id = {a.id: float(a.size) for a in allocations}
    conflicts = np.array(
        [len(overlaps.get(a.id, ())) for a in allocations], dtype=np.float64
    )
    load = size + np.array(
        [sum(size_by_id[i] for i in overlaps.get(a.id, ())) for a in allocations]
    )

    def norm(values: np.ndarray) -> np.ndarray:
        return values / max(values.max(), 1.0)

    def rank(values: np.ndarray) -> np.ndarray:
        order = np.argsort(values, kind="stable")
        ranks = np.empty(n, dtype=np.float64)
        ranks[order] = np.arange(n, dtype=np.float64)
        return ranks / max(n - 1, 1)

    local = np.stack(
        [
            norm(size),
            np.log1p(size) / max(np.log1p(size.max()), 1.0),
            (start - t0) / span,
            (end - t0) / span,
            duration / span,
            norm(area),
            conflicts / max(n - 1, 1),
            norm(load),
            rank(size),
            rank(duration),
            rank(area),
            rank((conflicts + 1.0) * size),
            # Lexicographic (start, -size) rank: integer starts spaced by
            # n+1 leave room for the [0, 1] size-rank tiebreak.
            rank(start * (n + 1.0) - rank(size)),
        ],
        axis=1,
    )

    lower_bound = max(peak_lower_bound(allocations), 1)
    peaks = np.array(
        [placer.evaluate(order) for order in classic_orders(allocations)],
        dtype=np.float64,
    )
    relative = np.clip(peaks / lower_bound - 1.0, 0.0, 2.0) / 2.0
    winner = np.zeros(len(peaks))
    winner[int(np.argmin(peaks))] = 1.0
    instance = np.tile(np.concatenate([relative, winner]), (n, 1))

    return np.concatenate([local, instance], axis=1).astype(np.float32)


class NeuralAllocator(BaseAllocator):
    """Learned score-and-sort allocator with a Transformer priority model.

    A permutation-equivariant Transformer scores each allocation and the
    allocations are placed first-fit in descending score order (the order
    fully determines the placement). Optionally evaluates `num_samples`
    Gumbel-perturbed orders from the same policy and keeps the best result,
    bounded by `max_seconds`. With `portfolio` the classic greedy orders are
    also evaluated as candidates, so results never trail greedy_by_all.
    Weights are trained by Plackett-Luce behavior cloning plus self-imitation
    (see training/train_neural.py); the default checkpoint ships with the
    package.
    """

    def __init__(
        self,
        weights: str | Path | None = None,
        num_samples: int = 1024,
        temperature: float = 1.0,
        seed: int = 42,
        max_seconds: float = DEFAULT_MAX_SECONDS,
        portfolio: bool = True,
    ) -> None:
        if not HAS_TORCH:
            require_optional("torch", "NeuralAllocator", "ml")
        if num_samples < 0:
            raise ValueError(f"num_samples must be non-negative, got {num_samples}")
        if temperature <= 0:
            raise ValueError(f"temperature must be positive, got {temperature}")
        if max_seconds < 0:
            raise ValueError(f"max_seconds must be non-negative, got {max_seconds}")

        self._weights = Path(weights) if weights is not None else DEFAULT_WEIGHTS
        self._num_samples = num_samples
        self._temperature = temperature
        self._seed = seed
        self._max_seconds = max_seconds
        self._portfolio = portfolio
        self._model: Any = None

    def _load_model(self) -> Any:  # noqa: ANN401
        if self._model is None:
            from .neural_model import load_model

            if not self._weights.is_file():
                raise FileNotFoundError(
                    f"Neural allocator weights not found at {self._weights}. "
                    f"Train them with training/train_neural.py --install"
                )
            self._model = load_model(self._weights)
        return self._model

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations
        require_unique_ids(allocations)

        model = self._load_model()
        placer = FirstFitPlacer(list(allocations))
        features = extract_features(allocations, placer)

        with torch.inference_mode():
            scores = model(torch.from_numpy(features).unsqueeze(0)).squeeze(0)
            best_order = torch.argsort(scores, descending=True).tolist()
            best_peak = placer.evaluate(best_order)

            if self._portfolio:
                for order in classic_orders(allocations):
                    peak = placer.evaluate(order)
                    if peak < best_peak:
                        best_order, best_peak = order, peak

            if self._num_samples:
                from .neural_model import sample_orders

                deadline = (
                    time.monotonic() + self._max_seconds if self._max_seconds else None
                )
                generator = torch.Generator().manual_seed(self._seed)
                orders = sample_orders(
                    scores.unsqueeze(0),
                    self._num_samples,
                    generator=generator,
                    temperature=self._temperature,
                ).squeeze(0)
                for order in orders.tolist():
                    if deadline is not None and time.monotonic() >= deadline:
                        break
                    peak = placer.evaluate(order)
                    if peak < best_peak:
                        best_order, best_peak = order, peak

        return tuple(placer.place(best_order))
