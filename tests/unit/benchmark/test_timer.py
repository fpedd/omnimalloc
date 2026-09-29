#
# SPDX-License-Identifier: Apache-2.0
#

import time

from omnimalloc.benchmark.timer import Timer


def test_timer_measures_its_block() -> None:
    with Timer() as timer:
        time.sleep(0.01)
    assert timer.elapsed_s >= 0.01
    assert timer.elapsed_s == timer.elapsed_ns / 1e9


def test_timer_freezes_on_exit() -> None:
    with Timer() as timer:
        pass
    elapsed = timer.elapsed_ns
    time.sleep(0.001)
    assert timer.elapsed_ns == elapsed


def test_timer_nests() -> None:
    with Timer() as outer, Timer() as inner:
        time.sleep(0.001)
    assert outer.elapsed_ns >= inner.elapsed_ns > 0
