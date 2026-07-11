#
# SPDX-License-Identifier: Apache-2.0
#

import numpy as np
import pytest
from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.allocators.neural import (
    HAS_TORCH,
    NeuralAllocator,
    extract_features,
    peak_lower_bound,
)
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool
from omnimalloc.validate import validate_allocation

pytestmark = pytest.mark.skipif(not HAS_TORCH, reason="torch not installed")


def _allocs() -> tuple[Allocation, ...]:
    return (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=5, end=15),
        Allocation(id=3, size=200, start=8, end=20),
        Allocation(id=4, size=25, start=12, end=30),
    )


def test_peak_lower_bound_empty() -> None:
    assert peak_lower_bound(()) == 0


def test_peak_lower_bound_disjoint() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=10, end=20),
    )
    assert peak_lower_bound(allocs) == 100


def test_peak_lower_bound_overlapping() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=5, end=15),
    )
    assert peak_lower_bound(allocs) == 150


def test_extract_features_shape_and_range() -> None:
    from omnimalloc.allocators.neural_model import FEATURE_DIM

    allocs = _allocs()
    placer = FirstFitPlacer(list(allocs))
    features = extract_features(allocs, placer)
    assert features.shape == (4, FEATURE_DIM)
    assert features.dtype == np.float32
    assert np.all(features >= 0.0)
    assert np.all(features <= 1.0)


def test_extract_features_single_allocation() -> None:
    from omnimalloc.allocators.neural_model import FEATURE_DIM

    allocs = (Allocation(id=1, size=100, start=0, end=10),)
    placer = FirstFitPlacer(list(allocs))
    features = extract_features(allocs, placer)
    assert features.shape == (1, FEATURE_DIM)
    assert np.all(np.isfinite(features))


def test_neural_rejects_negative_samples() -> None:
    with pytest.raises(ValueError, match="num_samples must be non-negative"):
        NeuralAllocator(num_samples=-1)


def test_neural_rejects_bad_temperature() -> None:
    with pytest.raises(ValueError, match="temperature must be positive"):
        NeuralAllocator(temperature=0.0)


def test_neural_rejects_negative_max_seconds() -> None:
    with pytest.raises(ValueError, match="max_seconds must be non-negative"):
        NeuralAllocator(max_seconds=-1.0)


def test_neural_missing_weights() -> None:
    allocator = NeuralAllocator(weights="/nonexistent/weights.pt")
    with pytest.raises(FileNotFoundError, match="weights not found"):
        allocator.allocate(_allocs())


def test_neural_empty() -> None:
    allocator = NeuralAllocator()
    assert allocator.allocate(()) == ()


def test_neural_single() -> None:
    allocator = NeuralAllocator()
    result = allocator.allocate((Allocation(id=1, size=100, start=0, end=10),))
    assert len(result) == 1
    assert result[0].offset == 0


def test_neural_produces_valid_allocation() -> None:
    allocator = NeuralAllocator()
    result = allocator.allocate(_allocs())
    assert len(result) == 4
    assert {a.id for a in result} == {1, 2, 3, 4}
    assert validate_allocation(Pool(id="test_pool", allocations=result))


def test_neural_deterministic() -> None:
    allocator = NeuralAllocator()
    first = allocator.allocate(_allocs())
    second = allocator.allocate(_allocs())
    assert [a.offset for a in first] == [a.offset for a in second]


def test_neural_rejects_duplicate_ids() -> None:
    allocator = NeuralAllocator()
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=1, size=50, start=5, end=15),
    )
    with pytest.raises(ValueError, match="allocation ids must be unique"):
        allocator.allocate(allocs)


def test_neural_greedy_decode_only() -> None:
    allocator = NeuralAllocator(num_samples=0)
    result = allocator.allocate(_allocs())
    assert validate_allocation(Pool(id="test_pool", allocations=result))


def test_sample_orders_are_distinct_valid_permutations() -> None:
    import torch
    from omnimalloc.allocators.neural_model import sample_orders

    scores = torch.randn(2, 30)
    generator = torch.Generator().manual_seed(0)
    orders = sample_orders(scores, 8, generator=generator)
    assert orders.shape == (2, 8, 30)
    assert all(sorted(orders[0, k].tolist()) == list(range(30)) for k in range(8))
    unique = {tuple(orders[0, k].tolist()) for k in range(8)}
    assert len(unique) > 1


def test_sample_orders_temperature_changes_orders() -> None:
    import torch
    from omnimalloc.allocators.neural_model import sample_orders

    scores = torch.randn(1, 40)
    cold = sample_orders(
        scores, 4, generator=torch.Generator().manual_seed(0), temperature=0.1
    )
    hot = sample_orders(
        scores, 4, generator=torch.Generator().manual_seed(0), temperature=10.0
    )
    assert not torch.equal(cold, hot)


def test_order_log_prob_finite_and_uniform_for_equal_scores() -> None:
    import math

    import torch
    from omnimalloc.allocators.neural_model import order_log_prob, sample_orders

    scores = torch.zeros(1, 20)
    orders = sample_orders(scores, 4, generator=torch.Generator().manual_seed(0))
    log_probs = order_log_prob(scores, orders)
    assert torch.isfinite(log_probs).all()
    expected = -sum(math.log(i) for i in range(1, 21))
    assert torch.allclose(log_probs, torch.full_like(log_probs, expected), atol=1e-3)


def test_neural_portfolio_never_trails_greedy_by_all() -> None:
    from omnimalloc.allocators.greedy import GreedyByAllAllocator
    from omnimalloc.allocators.greedy_base import peak_memory
    from omnimalloc.benchmark.sources import HighContentionSource

    allocs = HighContentionSource(num_allocations=40, seed=11).get_allocations()
    neural_peak = peak_memory(NeuralAllocator().allocate(allocs))
    greedy_peak = peak_memory(GreedyByAllAllocator(cores=1).allocate(allocs))
    assert neural_peak <= greedy_peak


def test_neural_respects_lower_bound_on_larger_problem() -> None:
    from omnimalloc.benchmark.sources import RandomSource

    allocs = RandomSource(num_allocations=60, seed=7).get_allocations()
    allocator = NeuralAllocator()
    result = allocator.allocate(allocs)
    assert validate_allocation(Pool(id="test_pool", allocations=result))
    peak = max(a.offset + a.size for a in result if a.offset is not None)
    assert peak >= peak_lower_bound(allocs)
