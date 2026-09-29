#
# SPDX-License-Identifier: Apache-2.0
#

from typing import Any

try:
    from tqdm.auto import tqdm
except ImportError:

    def tqdm(iterable: Any, **_: Any) -> Any:  # noqa: ANN401
        """No-op tqdm fallback when tqdm is not installed."""
        return iterable
