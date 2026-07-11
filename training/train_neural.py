#
# SPDX-License-Identifier: Apache-2.0
#

"""Train the NeuralAllocator priority model.

The model scores allocations; sorting by score gives a placement order that
first-fit placement turns into a full solution. Training has two phases:

1. Pretraining: behavior-clone the best classic greedy order per instance via
   the exact Plackett-Luce log-likelihood of the target permutation.
2. Self-imitation (expert iteration): sample orders from the current policy
   (Gumbel-perturbed scores), evaluate their peak memory with the C++
   FirstFitPlacer, keep the best order ever seen per instance (the incumbent,
   seeded with the greedy order), and imitate the incumbents. Costs are
   normalized by the max-live-load lower bound, a true optimality gap proxy.

Usage:
    uv run python training/train_neural.py --steps 10000 --install
"""

import argparse
import csv
import math
import random
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import torch
from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.allocators.neural import (
    DEFAULT_WEIGHTS,
    classic_orders,
    extract_features,
    peak_lower_bound,
)
from omnimalloc.allocators.neural_model import (
    PriorityNet,
    order_log_prob,
    sample_orders,
    save_model,
)
from omnimalloc.benchmark.sources import (
    BaseSource,
    HighContentionSource,
    PinwheelSource,
    PowerOf2Source,
    RandomSource,
    SequentialSource,
    TilingSource,
    UniformSource,
)
from omnimalloc.primitives import Allocation


@dataclass
class Instance:
    allocations: tuple[Allocation, ...]
    placer: FirstFitPlacer
    features: torch.Tensor
    lower_bound: int
    greedy_best: int
    greedy_order: list[int]
    best_peak: int
    best_order: list[int]


def _random_source(rng: random.Random, n: int, seed: int) -> BaseSource:
    """Sample a source with randomized parameters for training diversity."""
    kind = rng.choice(
        (
            "random",
            "random",
            "uniform",
            "power_of_2",
            "contention",
            "sequential",
            "tiling",
            "pinwheel",
        )
    )
    if kind == "random":
        time_max = rng.choice((100, 1000, 10000))
        return RandomSource(
            num_allocations=n,
            size_min=1,
            size_max=10 ** rng.randint(2, 7),
            time_max=time_max,
            duration_min=1,
            duration_max=rng.randint(2, max(2, time_max // 2)),
            seed=seed,
        )
    if kind == "uniform":
        time_max = rng.randint(20, 200)
        return UniformSource(
            num_allocations=n,
            size=2 ** rng.randint(4, 20),
            duration=rng.randint(1, time_max),
            time_max=time_max,
            seed=seed,
        )
    if kind == "power_of_2":
        exp_min = rng.randint(0, 10)
        return PowerOf2Source(
            num_allocations=n,
            size_exponent_min=exp_min,
            size_exponent_max=exp_min + rng.randint(1, 12),
            time_max=100,
            duration_min=1,
            duration_max=rng.randint(2, 80),
            seed=seed,
        )
    if kind == "contention":
        return HighContentionSource(
            num_allocations=n,
            size_min=1,
            size_max=10 ** rng.randint(2, 6),
            time_window=rng.randint(4, 64),
            seed=seed,
        )
    if kind == "sequential":
        return SequentialSource(
            num_allocations=n,
            size_min=1,
            size_max=10 ** rng.randint(2, 6),
            duration_min=1,
            duration_max=rng.randint(2, 30),
            seed=seed,
        )
    cls = TilingSource if kind == "tiling" else PinwheelSource
    return cls(
        num_allocations=n,
        capacity=2 ** rng.randint(10, 24),
        makespan=2 ** rng.randint(6, 16),
        min_size=1,
        min_duration=1,
        seed=seed,
    )


def _greedy_best(
    allocations: tuple[Allocation, ...], placer: FirstFitPlacer
) -> tuple[int, list[int]]:
    """Best peak across the classic greedy orders, with a consistent teacher.

    The teacher order is the first ordering within 1% of the best peak, so
    near-ties always resolve to the same heuristic and the cloning targets
    stay consistent across instances.
    """
    orders = classic_orders(allocations)
    peaks = [placer.evaluate(order) for order in orders]
    best_peak = min(peaks)
    teacher = next(
        order
        for order, peak in zip(orders, peaks, strict=True)
        if peak <= best_peak * 1.01
    )
    return best_peak, teacher


def make_instance(rng: random.Random, n_min: int, n_max: int) -> Instance:
    n = round(math.exp(rng.uniform(math.log(n_min), math.log(n_max))))
    source = _random_source(rng, n, seed=rng.randrange(1 << 30))
    allocations = source.get_allocations()
    placer = FirstFitPlacer(list(allocations))
    features = torch.from_numpy(extract_features(allocations, placer))
    greedy_best, greedy_order = _greedy_best(allocations, placer)
    return Instance(
        allocations=allocations,
        placer=placer,
        features=features,
        lower_bound=max(peak_lower_bound(allocations), 1),
        greedy_best=greedy_best,
        greedy_order=greedy_order,
        best_peak=greedy_best,
        best_order=list(greedy_order),
    )


def make_batch(
    instances: list[Instance],
) -> tuple[torch.Tensor, torch.Tensor]:
    """Pad features to a (B, N_max, F) tensor with a (B, N_max) validity mask."""
    n_max = max(inst.features.shape[0] for inst in instances)
    feature_dim = instances[0].features.shape[1]
    features = torch.zeros(len(instances), n_max, feature_dim)
    valid = torch.zeros(len(instances), n_max, dtype=torch.bool)
    for i, inst in enumerate(instances):
        n = inst.features.shape[0]
        features[i, :n] = inst.features
        valid[i, :n] = True
    return features, valid


def make_target_orders(instances: list[Instance], n_max: int) -> torch.Tensor:
    """(B, 1, N_max) winning greedy orders, padded indices appended at the end."""
    targets = torch.empty(len(instances), 1, n_max, dtype=torch.int64)
    for i, inst in enumerate(instances):
        n = len(inst.allocations)
        targets[i, 0, :n] = torch.tensor(inst.greedy_order)
        targets[i, 0, n:] = torch.arange(n, n_max)
    return targets


def evaluate_orders(instances: list[Instance], orders: torch.Tensor) -> torch.Tensor:
    """Peak/LB cost of each sampled order; padded indices sort last and drop.

    Also advances each instance's incumbent (best order found so far).
    """
    costs = torch.empty(orders.shape[:2])
    for i, inst in enumerate(instances):
        n = len(inst.allocations)
        for k in range(orders.shape[1]):
            order = orders[i, k, :n].tolist()
            peak = inst.placer.evaluate(order)
            costs[i, k] = peak / inst.lower_bound
            if peak < inst.best_peak:
                inst.best_peak, inst.best_order = peak, order
    return costs


def greedy_decode_cost(
    model: PriorityNet, instances: list[Instance], num_samples: int = 0
) -> tuple[float, float]:
    """Mean peak/LB of the deterministic decode and vs the greedy_by_all peak."""
    features, valid = make_batch(instances)
    with torch.inference_mode():
        scores = model(features, padding_mask=~valid)
        scores = scores.masked_fill(~valid, -torch.inf)
        orders = scores.argsort(dim=-1, descending=True)
        generator = torch.Generator().manual_seed(0)
        sampled = (
            sample_orders(scores, num_samples, generator=generator, valid_mask=valid)
            if num_samples
            else None
        )
    gaps, ratios = [], []
    for i, inst in enumerate(instances):
        n = len(inst.allocations)
        peak = inst.placer.evaluate(orders[i, :n].tolist())
        if sampled is not None:
            peak = min(
                peak,
                *(
                    inst.placer.evaluate(sampled[i, k, :n].tolist())
                    for k in range(sampled.shape[1])
                ),
            )
        gaps.append(peak / inst.lower_bound)
        ratios.append(peak / inst.greedy_best)
    return float(np.mean(gaps)), float(np.mean(ratios))


def pretrain(
    model: PriorityNet,
    pool: list[Instance],
    val_set: list[Instance],
    rng: random.Random,
    args: argparse.Namespace,
) -> None:
    """Behavior-clone the winning greedy order via Plackett-Luce likelihood.

    Starts REINFORCE from greedy_by_all quality instead of a random policy.
    """
    optimizer = torch.optim.Adam(model.parameters(), lr=args.pretrain_lr)
    for step in range(1, args.pretrain_steps + 1):
        instances = rng.sample(pool, args.batch_size)
        features, valid = make_batch(instances)
        targets = make_target_orders(instances, features.shape[1])

        scores = model(features, padding_mask=~valid)
        loss = -order_log_prob(scores, targets, valid_mask=valid).mean()

        optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()

        if step % args.eval_every == 0 or step == args.pretrain_steps:
            model.eval()
            val_gap, val_vs_greedy = greedy_decode_cost(model, val_set)
            model.train()
            print(
                f"pretrain {step:5d}  nll {float(loss.detach()):9.3f}  "
                f"val {val_gap:.4f}  vs_greedy {val_vs_greedy:.4f}"
            )


def self_imitation_step(
    model: PriorityNet,
    optimizer: torch.optim.Optimizer,
    instances: list[Instance],
    k: int,
) -> tuple[float, float, float]:
    """One self-imitation (expert-iteration) update.

    Samples k orders per instance from the current policy, advances each
    instance's incumbent, and takes a supervised Plackett-Luce likelihood step
    toward the incumbents. Incumbents start at the best greedy order and only
    improve, so this is a monotone hill-climb the policy generalizes across
    instances; the NLL loss is a meaningful, decreasing quantity.

    Returns NLL loss, mean incumbent peak/LB, and score std.
    """
    features, valid = make_batch(instances)

    scores = model(features, padding_mask=~valid)
    with torch.no_grad():
        masked = scores.masked_fill(~valid, -torch.inf)
        orders = sample_orders(masked, k, valid_mask=valid)
    evaluate_orders(instances, orders)

    targets = torch.empty(len(instances), 1, features.shape[1], dtype=torch.int64)
    for i, inst in enumerate(instances):
        n = len(inst.allocations)
        targets[i, 0, :n] = torch.tensor(inst.best_order)
        targets[i, 0, n:] = torch.arange(n, features.shape[1])
    loss = -order_log_prob(scores, targets, valid_mask=valid).mean()

    optimizer.zero_grad()
    loss.backward()
    torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
    optimizer.step()

    incumbent_cost = float(
        np.mean([inst.best_peak / inst.lower_bound for inst in instances])
    )
    score_std = float(scores.detach()[valid].std())
    return float(loss.detach()), incumbent_cost, score_std


def generate_data(args: argparse.Namespace) -> tuple[list[Instance], list[Instance]]:
    print(f"Generating {args.pool_size} training and {args.val_size} val instances")
    t0 = time.monotonic()
    rng = random.Random(args.seed + 500_009)
    pool = [make_instance(rng, args.n_min, args.n_max) for _ in range(args.pool_size)]
    val_rng = random.Random(args.seed + 1_000_003)
    val_set = [
        make_instance(val_rng, args.n_min, args.n_max) for _ in range(args.val_size)
    ]
    print(f"Instance generation took {time.monotonic() - t0:.1f}s")
    return pool, val_set


def train(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    torch.manual_seed(args.seed)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    pool, val_set = generate_data(args)
    val_greedy_gap = float(
        np.mean([inst.greedy_best / inst.lower_bound for inst in val_set])
    )
    print(f"Val greedy_by_all mean peak/LB: {val_greedy_gap:.4f}")

    model = PriorityNet(dim=args.dim, heads=args.heads, layers=args.layers)
    if args.resume:
        from omnimalloc.allocators.neural_model import load_model

        model = load_model(Path(args.resume))
        model.train()
    elif args.pretrain_steps:
        pretrain(model, pool, val_set, rng, args)
    optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, T_max=args.steps, eta_min=args.lr * 0.1
    )

    log_path = out_dir / "train_log.csv"
    best_val = float("inf")
    ema_cost: float | None = None
    with log_path.open("w", newline="") as log_file:
        writer = csv.writer(log_file)
        writer.writerow(
            [
                "step",
                "loss",
                "train_cost",
                "train_cost_ema",
                "val_gap",
                "val_vs_greedy",
                "score_std",
                "lr",
            ]
        )
        for step in range(1, args.steps + 1):
            instances = rng.sample(pool, args.batch_size)
            loss, train_cost, score_std = self_imitation_step(
                model, optimizer, instances, args.k
            )
            scheduler.step()
            ema_cost = (
                train_cost if ema_cost is None else 0.98 * ema_cost + 0.02 * train_cost
            )

            if step % args.eval_every == 0 or step == args.steps:
                model.eval()
                val_gap, val_vs_greedy = greedy_decode_cost(
                    model, val_set, num_samples=32
                )
                model.train()
                writer.writerow(
                    [
                        step,
                        f"{loss:.5f}",
                        f"{train_cost:.5f}",
                        f"{ema_cost:.5f}",
                        f"{val_gap:.5f}",
                        f"{val_vs_greedy:.5f}",
                        f"{score_std:.4f}",
                        f"{scheduler.get_last_lr()[0]:.2e}",
                    ]
                )
                log_file.flush()
                marker = ""
                if val_gap < best_val:
                    best_val = val_gap
                    save_model(model, out_dir / "best.pt")
                    marker = " *"
                print(
                    f"step {step:6d}  loss {loss:+.4f}  "
                    f"train {ema_cost:.4f}  val {val_gap:.4f}  "
                    f"vs_greedy {val_vs_greedy:.4f}  std {score_std:.2f}{marker}"
                )

    save_model(model, out_dir / "last.pt")
    print(f"Best val peak/LB: {best_val:.4f} (greedy_by_all: {val_greedy_gap:.4f})")

    if args.install:
        shutil.copyfile(out_dir / "best.pt", DEFAULT_WEIGHTS)
        print(f"Installed best checkpoint to {DEFAULT_WEIGHTS}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--steps", type=int, default=10000)
    parser.add_argument("--pretrain-steps", type=int, default=2000)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--k", type=int, default=16, help="permutation samples")
    parser.add_argument("--lr", type=float, default=3e-4)
    parser.add_argument("--pretrain-lr", type=float, default=1e-3)
    parser.add_argument("--dim", type=int, default=64)
    parser.add_argument("--heads", type=int, default=4)
    parser.add_argument("--layers", type=int, default=3)
    parser.add_argument("--pool-size", type=int, default=4096)
    parser.add_argument("--val-size", type=int, default=128)
    parser.add_argument("--n-min", type=int, default=12)
    parser.add_argument("--n-max", type=int, default=160)
    parser.add_argument("--eval-every", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=str, default="training/checkpoints")
    parser.add_argument("--resume", type=str, default=None)
    parser.add_argument(
        "--install", action="store_true", help="copy best checkpoint into the package"
    )
    train(parser.parse_args())


if __name__ == "__main__":
    main()
