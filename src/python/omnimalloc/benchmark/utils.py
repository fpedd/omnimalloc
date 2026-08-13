#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Any


class _DummyProgressBar:
    """No-op stand-in for tqdm's total=... progress bar."""

    def __enter__(self) -> "_DummyProgressBar":
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def update(self, n: int = 1) -> None:
        pass


def tqdm(iterable: Any = None, **kwargs: Any) -> Any:  # noqa: ANN401
    """Lazily imported tqdm.auto progress bar; a no-op when tqdm is not installed."""
    try:
        from tqdm.auto import tqdm as tqdm_auto
    except ImportError:
        if iterable is None:
            # When called with total= instead of an iterable
            return _DummyProgressBar()
        return iterable
    return tqdm_auto(iterable, **kwargs)
