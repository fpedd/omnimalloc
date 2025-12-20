#
# SPDX-License-Identifier: Apache-2.0
#

import logging
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from functools import wraps
from types import TracebackType
from typing import Any, TypeVar, cast

logger = logging.getLogger(__name__)


class Timer:
    """Lightweight timer for performance measurement.

    TODO(fpedd): This class is not yet thread-safe. Concurrent access from
    multiple threads may result in race conditions and inconsistent state.
    """

    def __init__(self, *, auto_start: bool = False) -> None:
        self._start_ns: int | None = None
        self._stop_ns: int | None = None
        self._is_running: bool = False

        if auto_start:
            self.start()

    def __enter__(self) -> "Timer":
        if not self._is_running:
            self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._is_running:
            self.stop()

    def start(self) -> "Timer":
        if self._is_running:
            raise RuntimeError("Timer is already running")
        self._start_ns = time.perf_counter_ns()
        self._stop_ns = None
        self._is_running = True
        return self

    def stop(self) -> "Timer":
        if not self._is_running:
            raise RuntimeError("Timer is not running")
        if self._start_ns is None:
            raise RuntimeError("Timer has no start time")
        self._stop_ns = time.perf_counter_ns()
        self._is_running = False
        return self

    def reset(self) -> "Timer":
        self._start_ns = None
        self._stop_ns = None
        self._is_running = False
        return self

    def current_ns(self) -> int:
        if not self._is_running:
            raise RuntimeError("Timer is not running")
        if self._start_ns is None:
            raise RuntimeError("Timer has no start time")
        return time.perf_counter_ns() - self._start_ns

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def elapsed_ns(self) -> int:
        if self._start_ns is None:
            raise RuntimeError("Timer has no start time")
        if self._is_running:
            return time.perf_counter_ns() - self._start_ns
        if self._stop_ns is None:
            raise RuntimeError("Timer has not been stopped yet")
        return self._stop_ns - self._start_ns

    @property
    def elapsed_us(self) -> float:
        return self.elapsed_ns / 1_000

    @property
    def elapsed_ms(self) -> float:
        return self.elapsed_ns / 1_000_000

    @property
    def elapsed_s(self) -> float:
        return self.elapsed_ns / 1_000_000_000

    @property
    def elapsed(self) -> str:
        return _format_time(self.elapsed_ns)


F = TypeVar("F", bound=Callable[..., Any])


def measure(func: F) -> F:
    """Decorator to measure function execution time."""

    @wraps(func)
    def wrapper(*args: object, **kwargs: object) -> object:
        with Timer(auto_start=True) as timer:
            result = func(*args, **kwargs)
        name = getattr(func, "__name__", "<unknown>")
        print(f"{name}: {timer.elapsed}")
        return result

    return cast("F", wrapper)


@contextmanager
def time_block(name: str) -> Generator[Timer, None, None]:
    """Time a named code block and print elapsed time on exit."""
    with Timer(auto_start=True) as timer:
        yield timer
    print(f"{name}: {timer.elapsed}")


def _format_time(ns: int) -> str:
    if ns < 1_000:
        return f"{ns} ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.2f} us"
    if ns < 1_000_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    if ns < 1_000_000_000_000:
        return f"{ns / 1_000_000_000:.2f} s"
    if ns < 60 * 1_000_000_000_000:
        return f"{ns / (1_000_000_000_000):.2f} min"
    return f"{ns / (60 * 1_000_000_000_000):.2f} h"
