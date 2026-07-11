# NeuralAllocator training

The `NeuralAllocator` is a learned score-and-sort policy: a small
permutation-equivariant Transformer (`PriorityNet`, ~100k parameters) scores
every allocation of a problem, and placing the allocations first-fit in
descending score order yields the full solution. The placement order alone
determines the outcome (the insight behind minimalloc's canonical solutions),
so the model only has to learn a good ordering — never raw offsets.

## Method

- **Features**: per-allocation size/start/end/duration/area/conflict-degree
  and their ranks, normalized per instance (scale-invariant), plus
  instance-level context broadcast to every allocation: the relative peak of
  each classic greedy order and a one-hot of the winner, so the model can
  condition on which heuristic family suits the instance.
- **Policy**: allocation scores define a Plackett-Luce distribution over
  permutations; sampling = Gumbel-perturb the scores and sort (Gumbel-top-k).
- **Pretraining**: behavior-cloning of the best classic greedy order per
  instance via the exact Plackett-Luce log-likelihood. Near-ties (within 1%)
  resolve to a fixed heuristic priority so the targets stay consistent.
- **Self-imitation fine-tuning** (expert iteration): sample `k` orders per
  instance from the policy, evaluate their peak memory with the C++
  `FirstFitPlacer`, keep the best order ever seen per instance (the
  incumbent, seeded with the greedy order), and imitate the incumbents.
  Costs are normalized by the max-live-load lower bound. Plain REINFORCE
  with a mean baseline was tried first and stagnated; imitating incumbents
  gives a monotone, low-variance signal that exploits the cheap evaluator.
- **Data**: freshly generated problems from the omnimalloc generator sources
  (random, uniform, power-of-2, high-contention, sequential, tiling, pinwheel)
  with randomized parameters. Minimalloc CSV datasets stay held out for eval.

At inference the allocator takes the best of the deterministic decode, the
classic greedy orders (portfolio guarantee: never worse than greedy_by_all),
and up to `num_samples` Gumbel-perturbed policy samples within `max_seconds`.

## Usage

```bash
# Train and install the best checkpoint into the package
uv run python training/train_neural.py --install

# Evaluate against the classic allocators on held-out problems
uv run python training/eval_neural.py
```

Training runs on CPU in well under an hour with the default settings.
Checkpoints and a CSV metrics log land in `training/checkpoints/`.
