#
# SPDX-License-Identifier: Apache-2.0
#


import io
import time
from contextlib import redirect_stdout

import pytest
from omnimalloc.benchmark.timer import Timer, measure, time_block


def test_init_default() -> None:
    """Test timer initialization with default parameters."""
    timer = Timer()
    assert not timer.is_running
    assert timer._start_ns is None  # noqa: SLF001
    assert timer._stop_ns is None  # noqa: SLF001


def test_init_auto_start() -> None:
    """Test timer initialization with auto_start=True."""
    timer = Timer(auto_start=True)
    assert timer.is_running
    assert timer._start_ns is not None  # noqa: SLF001
    assert timer._stop_ns is None  # noqa: SLF001


def test_start() -> None:
    """Test starting the timer."""
    timer = Timer()
    result = timer.start()
    assert timer.is_running
    assert timer._start_ns is not None  # noqa: SLF001
    assert result is timer


def test_start_already_running() -> None:
    """Test that starting an already running timer raises an error."""
    timer = Timer(auto_start=True)
    with pytest.raises(RuntimeError, match="Timer is already running"):
        timer.start()


def test_stop() -> None:
    """Test stopping the timer."""
    timer = Timer(auto_start=True)
    time.sleep(0.001)  # Sleep for 1ms
    result = timer.stop()
    assert not timer.is_running
    assert timer._stop_ns is not None  # noqa: SLF001
    assert timer.elapsed_ns > 0
    assert result is timer


def test_stop_not_running() -> None:
    """Test that stopping a non-running timer raises an error."""
    timer = Timer()
    with pytest.raises(RuntimeError, match="Timer is not running"):
        timer.stop()


def test_reset() -> None:
    """Test resetting the timer."""
    timer = Timer(auto_start=True)
    time.sleep(0.001)
    timer.stop()
    result = timer.reset()
    assert not timer.is_running
    assert timer._start_ns is None  # noqa: SLF001
    assert timer._stop_ns is None  # noqa: SLF001
    assert result is timer


def test_current_ns() -> None:
    """Test current_ns functionality."""
    timer = Timer(auto_start=True)
    time.sleep(0.001)
    current1 = timer.current_ns()
    time.sleep(0.001)
    current2 = timer.current_ns()
    assert current1 > 0
    assert current2 > current1


def test_current_ns_not_running() -> None:
    """Test that current_ns on a non-running timer raises an error."""
    timer = Timer()
    with pytest.raises(RuntimeError, match="Timer is not running"):
        timer.current_ns()


def test_is_running_property() -> None:
    """Test is_running property."""
    timer = Timer()
    assert not timer.is_running
    timer.start()
    assert timer.is_running
    timer.stop()
    assert not timer.is_running


def test_elapsed_ns_stopped() -> None:
    """Test elapsed_ns property on a stopped timer."""
    timer = Timer(auto_start=True)
    time.sleep(0.001)
    timer.stop()
    elapsed = timer.elapsed_ns
    assert elapsed > 1_000_000  # At least 1ms in nanoseconds
    # Reading again should return same value
    assert timer.elapsed_ns == elapsed


def test_elapsed_ns_running() -> None:
    """Test elapsed_ns property on a running timer."""
    timer = Timer(auto_start=True)
    time.sleep(0.001)
    elapsed1 = timer.elapsed_ns
    time.sleep(0.001)
    elapsed2 = timer.elapsed_ns
    assert elapsed1 > 0
    assert elapsed2 > elapsed1  # Should increase while running


def test_elapsed_ns_never_started() -> None:
    """Test elapsed_ns raises error if timer never stopped."""
    timer = Timer()
    with pytest.raises(RuntimeError, match="Timer has no start time"):
        _ = timer.elapsed_ns


def test_elapsed_us() -> None:
    """Test elapsed_us property (microseconds)."""
    timer = Timer(auto_start=True)
    time.sleep(0.001)
    timer.stop()
    elapsed_us = timer.elapsed_us
    elapsed_ns = timer.elapsed_ns
    assert abs(elapsed_us - elapsed_ns / 1_000) < 0.01


def test_elapsed_ms() -> None:
    """Test elapsed_ms property (milliseconds)."""
    timer = Timer(auto_start=True)
    time.sleep(0.001)
    timer.stop()
    elapsed_ms = timer.elapsed_ms
    elapsed_ns = timer.elapsed_ns
    assert abs(elapsed_ms - elapsed_ns / 1_000_000) < 0.01


def test_elapsed_s() -> None:
    """Test elapsed_s property (seconds)."""
    timer = Timer(auto_start=True)
    time.sleep(0.001)
    timer.stop()
    elapsed_s = timer.elapsed_s
    elapsed_ns = timer.elapsed_ns
    assert abs(elapsed_s - elapsed_ns / 1_000_000_000) < 0.000001


def test_elapsed_formatted() -> None:
    """Test elapsed property (human-readable format)."""
    timer = Timer(auto_start=True)
    time.sleep(0.001)
    timer.stop()
    elapsed = timer.elapsed
    assert isinstance(elapsed, str)
    # Should be either "X.XX ms" or "XXX.XX us" depending on timing
    assert "ms" in elapsed or "us" in elapsed


def test_context_manager_basic() -> None:
    """Test basic context manager usage."""
    with Timer() as timer:
        assert timer.is_running
        time.sleep(0.001)
    assert not timer.is_running
    assert timer.elapsed_ns > 0


def test_context_manager_auto_start() -> None:
    """Test context manager with auto_start parameter."""
    with Timer(auto_start=False) as timer:
        # Even with auto_start=False, __enter__ starts the timer
        assert timer.is_running


def test_context_manager_access_after() -> None:
    """Test accessing timer properties after context exits."""
    with Timer() as timer:
        time.sleep(0.001)
    elapsed = timer.elapsed_ns
    assert elapsed > 0
    assert timer.elapsed_ns == elapsed


def test_decorator_basic() -> None:
    """Test decorator."""
    captured_output = io.StringIO()

    @measure
    def test_func() -> str:
        time.sleep(0.001)
        return "done"

    with redirect_stdout(captured_output):
        result = test_func()

    assert result == "done"
    output = captured_output.getvalue()
    assert "test_func:" in output


def test_decorator_preserves_function_metadata() -> None:
    """Test that decorator preserves function name and docstring."""

    @measure
    def example_function() -> None:
        """Example docstring."""

    assert example_function.__name__ == "example_function"
    assert example_function.__doc__ == "Example docstring."


def test_time_block_basic() -> None:
    """Test basic time_block usage."""
    captured_output = io.StringIO()

    with redirect_stdout(captured_output):  # noqa: SIM117
        with time_block(
            "test operation",
        ) as t:
            assert t.is_running
            time.sleep(0.001)

    output = captured_output.getvalue()
    assert "test operation:" in output
    assert not t.is_running


def test_time_block_access_timer() -> None:
    """Test accessing timer during time_block execution."""
    with time_block("test") as timer:
        time.sleep(0.001)
        current1 = timer.current_ns()
        time.sleep(0.001)
        current2 = timer.current_ns()
        assert current2 > current1


def test_multiple_start_stop_cycles() -> None:
    """Test multiple start/stop cycles on the same timer."""
    timer = Timer()

    # First cycle
    timer.start()
    time.sleep(0.001)
    timer.stop()
    elapsed1 = timer.elapsed_ns

    # Second cycle (without reset)
    timer.start()
    time.sleep(0.001)
    timer.stop()
    elapsed2 = timer.elapsed_ns

    # Reset and try again
    timer.reset()
    timer.start()
    time.sleep(0.001)
    timer.stop()
    elapsed3 = timer.elapsed_ns

    assert elapsed1 > 0
    assert elapsed2 > 0
    assert elapsed3 > 0


def test_concurrent_timers() -> None:
    """Test that multiple timers can run independently."""
    timer1 = Timer(auto_start=True)
    time.sleep(0.002)
    timer2 = Timer(auto_start=True)
    time.sleep(0.002)
    timer1.stop()
    time.sleep(0.002)
    timer2.stop()

    assert timer1.is_running is False
    assert timer2.is_running is False
    # timer2 started later but ran longer
    # Allow for timing variance - just check both recorded time
    assert timer1.elapsed_ns > 0
    assert timer2.elapsed_ns > 0


def test_nested_context_managers() -> None:
    """Test nested timer context managers."""
    with Timer() as outer:
        time.sleep(0.001)
        with Timer() as inner:
            time.sleep(0.001)
        inner_elapsed = inner.elapsed_ns
        time.sleep(0.001)
    outer_elapsed = outer.elapsed_ns

    assert inner_elapsed > 0
    assert outer_elapsed > inner_elapsed
