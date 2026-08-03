#
# SPDX-License-Identifier: Apache-2.0
#

import time
from types import TracebackType


class Timer:
    """Lightweight timer for performance measurement.

    TODO(fpedd): This class is not yet thread-safe. Concurrent access from
    multiple threads may result in race conditions and inconsistent state.
    """

    def __init__(self) -> None:
        self._start_ns: int | None = None
        self._stop_ns: int | None = None

    def __enter__(self) -> "Timer":
        if not self.is_running:
            self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.is_running:
            self.stop()

    def start(self) -> "Timer":
        if self.is_running:
            raise RuntimeError("Timer is already running")
        self._start_ns = time.perf_counter_ns()
        self._stop_ns = None
        return self

    def stop(self) -> "Timer":
        if not self.is_running:
            raise RuntimeError("Timer is not running")
        self._stop_ns = time.perf_counter_ns()
        return self

    @property
    def is_running(self) -> bool:
        return self._start_ns is not None and self._stop_ns is None

    @property
    def elapsed_ns(self) -> int:
        if self._start_ns is None:
            raise RuntimeError("Timer has no start time")
        if self._stop_ns is None:
            return time.perf_counter_ns() - self._start_ns
        return self._stop_ns - self._start_ns

    @property
    def elapsed_s(self) -> float:
        return self.elapsed_ns / 1_000_000_000

    @property
    def elapsed(self) -> str:
        return _format_time(self.elapsed_ns)


def _format_time(ns: int) -> str:
    if ns < 1_000:
        return f"{ns} ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.2f} us"
    if ns < 1_000_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    if ns < 60 * 1_000_000_000:
        return f"{ns / 1_000_000_000:.2f} s"
    if ns < 3_600 * 1_000_000_000:
        return f"{ns / (60 * 1_000_000_000):.2f} min"
    return f"{ns / (3_600 * 1_000_000_000):.2f} h"
