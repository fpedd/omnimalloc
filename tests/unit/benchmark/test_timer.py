#
# SPDX-License-Identifier: Apache-2.0
#


import time

import pytest
from omnimalloc.benchmark.timer import Timer, _format_time


def test_init_default() -> None:
    timer = Timer()
    assert not timer.is_running
    assert timer._start_ns is None  # noqa: SLF001
    assert timer._stop_ns is None  # noqa: SLF001


def test_start() -> None:
    timer = Timer()
    result = timer.start()
    assert timer.is_running
    assert timer._start_ns is not None  # noqa: SLF001
    assert result is timer


def test_start_already_running() -> None:
    timer = Timer()
    timer.start()
    with pytest.raises(RuntimeError, match="Timer is already running"):
        timer.start()


def test_stop() -> None:
    timer = Timer()
    timer.start()
    time.sleep(0.001)
    result = timer.stop()
    assert not timer.is_running
    assert timer._stop_ns is not None  # noqa: SLF001
    assert timer.elapsed_ns > 0
    assert result is timer


def test_stop_not_running() -> None:
    timer = Timer()
    with pytest.raises(RuntimeError, match="Timer is not running"):
        timer.stop()


def test_is_running_property() -> None:
    timer = Timer()
    assert not timer.is_running
    timer.start()
    assert timer.is_running
    timer.stop()
    assert not timer.is_running


def test_elapsed_ns_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    ticks = iter([1_000, 2_000_000])
    monkeypatch.setattr(time, "perf_counter_ns", lambda: next(ticks))
    timer = Timer()
    timer.start()
    timer.stop()
    elapsed = timer.elapsed_ns
    assert elapsed == 1_999_000
    assert timer.elapsed_ns == elapsed


def test_elapsed_ns_running() -> None:
    timer = Timer()
    timer.start()
    time.sleep(0.001)
    elapsed1 = timer.elapsed_ns
    time.sleep(0.001)
    elapsed2 = timer.elapsed_ns
    assert elapsed1 > 0
    assert elapsed2 > elapsed1


def test_elapsed_ns_never_started() -> None:
    timer = Timer()
    with pytest.raises(RuntimeError, match="Timer has no start time"):
        _ = timer.elapsed_ns


def test_elapsed_s() -> None:
    timer = Timer()
    timer.start()
    time.sleep(0.001)
    timer.stop()
    assert timer.elapsed_s == pytest.approx(timer.elapsed_ns / 1_000_000_000)


def test_elapsed_formatted() -> None:
    timer = Timer()
    timer.start()
    time.sleep(0.001)
    timer.stop()
    elapsed = timer.elapsed
    assert isinstance(elapsed, str)
    assert "ms" in elapsed or "us" in elapsed


def test_context_manager_basic() -> None:
    with Timer() as timer:
        assert timer.is_running
        time.sleep(0.001)
    assert not timer.is_running
    assert timer.elapsed_ns > 0


def test_context_manager_access_after() -> None:
    with Timer() as timer:
        time.sleep(0.001)
    elapsed = timer.elapsed_ns
    assert elapsed > 0
    assert timer.elapsed_ns == elapsed


def test_restart_without_reset() -> None:
    timer = Timer()
    timer.start()
    time.sleep(0.001)
    timer.stop()
    elapsed1 = timer.elapsed_ns
    timer.start()
    time.sleep(0.001)
    timer.stop()
    assert elapsed1 > 0
    assert timer.elapsed_ns > 0


def test_nested_context_managers() -> None:
    with Timer() as outer:
        time.sleep(0.001)
        with Timer() as inner:
            time.sleep(0.001)
        inner_elapsed = inner.elapsed_ns
        time.sleep(0.001)
    outer_elapsed = outer.elapsed_ns

    assert inner_elapsed > 0
    assert outer_elapsed > inner_elapsed


def test_format_time_minutes_and_hours() -> None:
    assert _format_time(90 * 10**9) == "1.50 min"
    assert _format_time(30 * 60 * 10**9) == "30.00 min"
    assert _format_time(2 * 3600 * 10**9) == "2.00 h"
