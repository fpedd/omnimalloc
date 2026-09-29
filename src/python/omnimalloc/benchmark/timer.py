#
# SPDX-License-Identifier: Apache-2.0
#

import time
from types import TracebackType


class Timer:
    """Context manager measuring the wall time of its block."""

    elapsed_ns: int = 0

    def __enter__(self) -> "Timer":
        self._start_ns = time.perf_counter_ns()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.elapsed_ns = time.perf_counter_ns() - self._start_ns

    @property
    def elapsed_s(self) -> float:
        return self.elapsed_ns / 1e9
