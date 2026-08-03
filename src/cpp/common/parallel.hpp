//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <thread>
#include <vector>

#ifdef __linux__
#include <sched.h>
#endif

namespace omnimalloc {

// Parallelism kicks in only where work dwarfs thread startup cost
inline constexpr size_t kMinParallel = 512;

// Dynamic scheduling hands out rows in blocks of this size
inline constexpr size_t kRowBlock = 32;

// Cores this process may actually run on. `hardware_concurrency` reports the
// machine, so under an affinity mask or a CPU-limited container it oversizes
// every kernel. Read per call, so a mask changing mid-run is honored.
inline unsigned usable_cores() noexcept {
#ifdef __linux__
  cpu_set_t set;
  if (sched_getaffinity(0, sizeof(set), &set) == 0) {
    const int count = CPU_COUNT(&set);
    if (count > 0) {
      return static_cast<unsigned>(count);
    }
  }
#endif
  return std::max(1u, std::thread::hardware_concurrency());
}

// Ceiling on the workers any one kernel spawns. Kernels spawn per call with no
// shared pool, so nothing else bounds N concurrent callers times the core
// count. A caller that owns the machine should raise the default of 8.
inline constexpr unsigned kDefaultMaxThreads = 8;

inline std::atomic<unsigned>& max_threads_slot() noexcept {
  static std::atomic<unsigned> slot{kDefaultMaxThreads};
  return slot;
}

// 0 lifts the ceiling to every usable core.
inline void set_max_threads(unsigned value) noexcept {
  max_threads_slot().store(value, std::memory_order_relaxed);
}

// The ceiling in force, never above what this process may actually use.
inline unsigned max_threads() noexcept {
  const unsigned value = max_threads_slot().load(std::memory_order_relaxed);
  const unsigned cores = usable_cores();
  return value == 0 ? cores : std::min(value, cores);
}

// Workers for a sweep of `n` cheap rows: none below kMinParallel, and never
// more than row blocks, which would only pay spawn cost. Kernels whose single
// rows carry heavy work size themselves against max_threads() directly.
inline unsigned parallel_threads(size_t n) {
  if (n < kMinParallel) {
    return 1;
  }
  const size_t blocks = (n + kRowBlock - 1) / kRowBlock;
  return static_cast<unsigned>(
      std::min(static_cast<size_t>(max_threads()), blocks));
}

// Lock-free max and min accumulation; relaxed ordering suffices for
// reductions that are joined before any read.
template <typename T>
inline void atomic_fetch_max(std::atomic<T>& target, T value) noexcept {
  T current = target.load(std::memory_order_relaxed);
  while (current < value && !target.compare_exchange_weak(
                                current, value, std::memory_order_relaxed)) {
  }
}

template <typename T>
inline void atomic_fetch_min(std::atomic<T>& target, T value) noexcept {
  T current = target.load(std::memory_order_relaxed);
  while (value < current && !target.compare_exchange_weak(
                                current, value, std::memory_order_relaxed)) {
  }
}

// Dynamic row blocks: per-row costs vary wildly under pruning, so static
// partitioning would leave threads idle
template <typename RowBody>
void for_each_row_block(size_t n, unsigned num_threads, RowBody&& row_body,
                        size_t block = kRowBlock) {
  if (num_threads <= 1) {
    for (size_t row = 0; row < n; ++row) {
      row_body(row);
    }
    return;
  }
  std::atomic<size_t> next{0};
  const auto worker = [&] {
    while (true) {
      const size_t begin = next.fetch_add(1) * block;
      if (begin >= n) {
        return;
      }
      const size_t end = std::min(n, begin + block);
      for (size_t row = begin; row < end; ++row) {
        row_body(row);
      }
    }
  };
  std::vector<std::future<void>> futures;
  futures.reserve(num_threads - 1);
  for (unsigned t = 1; t < num_threads; ++t) {
    futures.push_back(std::async(std::launch::async, worker));
  }
  worker();
  for (auto& future : futures) {
    future.get();
  }
}

}  // namespace omnimalloc
