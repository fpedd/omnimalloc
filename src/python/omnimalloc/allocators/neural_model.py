#
# SPDX-License-Identifier: Apache-2.0
#

"""Torch model and Plackett-Luce utilities for the neural allocator.

This module requires torch; import it lazily (see `neural.py`).
"""

from pathlib import Path
from typing import Final

import torch
from torch import Tensor, nn

# Number of per-allocation input features (see neural.extract_features):
# 13 local (normalized magnitudes and ranks) + 14 instance-level (relative
# peaks of the classic greedy orders and a one-hot of the winner).
FEATURE_DIM: Final[int] = 27

# Scores are bounded to +-SCORE_CLIP via tanh so the Plackett-Luce policy
# never becomes deterministic and exploration survives long training runs.
SCORE_CLIP: Final[float] = 10.0

# Effectively -inf for masked logits, small enough to avoid NaNs in logcumsumexp
_NEG: Final[float] = -1e9


class PriorityNet(nn.Module):
    """Permutation-equivariant encoder emitting one priority score per allocation.

    A Transformer encoder without positional encoding treats the input as a
    set; sorting allocations by the emitted scores yields the placement order.
    """

    def __init__(
        self,
        feature_dim: int = FEATURE_DIM,
        dim: int = 64,
        heads: int = 4,
        layers: int = 3,
    ) -> None:
        super().__init__()
        self.config = {  # type: ignore
            "feature_dim": feature_dim,
            "dim": dim,
            "heads": heads,
            "layers": layers,
        }
        self.embed = nn.Linear(feature_dim, dim)
        layer = nn.TransformerEncoderLayer(
            d_model=dim,
            nhead=heads,
            dim_feedforward=2 * dim,
            dropout=0.0,
            batch_first=True,
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(
            layer, num_layers=layers, enable_nested_tensor=False
        )
        self.head = nn.Sequential(
            nn.LayerNorm(dim), nn.Linear(dim, dim), nn.GELU(), nn.Linear(dim, 1)
        )

    def forward(self, features: Tensor, padding_mask: Tensor | None = None) -> Tensor:
        """Map (B, N, F) features to (B, N) bounded priority scores."""
        x = self.embed(features)
        x = self.encoder(x, src_key_padding_mask=padding_mask)
        scores = self.head(x).squeeze(-1)
        return SCORE_CLIP * torch.tanh(scores)


def sample_orders(
    scores: Tensor,
    num_samples: int,
    generator: torch.Generator | None = None,
    valid_mask: Tensor | None = None,
    temperature: float = 1.0,
) -> Tensor:
    """Sample (B, K, N) placement orders from the Plackett-Luce policy.

    Perturbing logits with Gumbel noise and sorting descending is an exact
    Plackett-Luce sample (Gumbel-top-k trick). Padded positions sort last.
    """
    logits = scores.detach() / temperature
    uniform = torch.rand(
        (*logits.shape[:-1], num_samples, logits.shape[-1]),
        generator=generator,
        device=logits.device,
    )
    exponential = -torch.log(uniform.clamp_min(1e-20))
    gumbel = -torch.log(exponential.clamp_min(1e-20))
    perturbed = logits.unsqueeze(-2) + gumbel
    if valid_mask is not None:
        perturbed = perturbed.masked_fill(~valid_mask.unsqueeze(-2), _NEG)
    return perturbed.argsort(dim=-1, descending=True)


def order_log_prob(
    scores: Tensor, orders: Tensor, valid_mask: Tensor | None = None
) -> Tensor:
    """Exact Plackett-Luce log-probability of (B, K, N) orders under (B, N) scores.

    log p(order) = sum_i [s_{o_i} - logsumexp(s_{o_i}, ..., s_{o_N})], computed
    with a reversed cumulative logsumexp over the ordered scores.
    """
    expanded = scores.unsqueeze(-2).expand(*orders.shape)
    ordered = expanded.gather(-1, orders)
    if valid_mask is not None:
        expanded_valid = valid_mask.unsqueeze(-2).expand(*orders.shape)
        ordered_valid = expanded_valid.gather(-1, orders)
        ordered = ordered.masked_fill(~ordered_valid, _NEG)
    tail_lse = torch.logcumsumexp(ordered.flip(-1), dim=-1).flip(-1)
    log_probs = ordered - tail_lse
    if valid_mask is not None:
        log_probs = log_probs.masked_fill(~ordered_valid, 0.0)
    return log_probs.sum(-1)


def save_model(model: PriorityNet, path: Path) -> None:
    """Save config and weights; float16 halves the shipped checkpoint size."""
    state = {key: value.half() for key, value in model.state_dict().items()}
    torch.save({"config": model.config, "state_dict": state}, path)


def load_model(path: Path) -> PriorityNet:
    checkpoint = torch.load(path, map_location="cpu", weights_only=True)
    model = PriorityNet(**checkpoint["config"])
    state = {key: value.float() for key, value in checkpoint["state_dict"].items()}
    model.load_state_dict(state)
    model.eval()
    return model
