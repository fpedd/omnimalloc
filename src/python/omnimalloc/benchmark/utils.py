#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Any

try:
    from tqdm.auto import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

    class _DummyProgressBar:
        """No-op stand-in for tqdm's total=... progress bar."""

        def __enter__(self) -> "_DummyProgressBar":
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def update(self, n: int = 1) -> None:
            pass

    def tqdm(iterable: Any = None, **kwargs: Any) -> Any:  # noqa: ARG001, ANN401
        """No-op tqdm fallback when tqdm is not installed."""
        if iterable is None:
            # When called with total= instead of an iterable
            return _DummyProgressBar()
        return iterable
