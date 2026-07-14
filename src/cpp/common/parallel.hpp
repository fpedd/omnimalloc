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

namespace omnimalloc {

// Parallelism kicks in only where work dwarfs thread startup cost
inline constexpr size_t kMinParallel = 512;

// `min_parallel` is the row count where threads start paying off; lower it
// when single rows carry heavy work (e.g. one flow solve per row).
inline unsigned parallel_threads(size_t n, size_t min_parallel = kMinParallel) {
  if (n < min_parallel) {
    return 1;
  }
  return std::min(8u, std::max(1u, std::thread::hardware_concurrency()));
}

// Lock-free max accumulation; relaxed ordering suffices for reductions that
// are joined before any read.
inline void atomic_fetch_max(std::atomic<int64_t>& target,
                             int64_t value) noexcept {
  int64_t current = target.load(std::memory_order_relaxed);
  while (current < value && !target.compare_exchange_weak(
                                current, value, std::memory_order_relaxed)) {
  }
}

// Dynamic row blocks: per-row costs vary wildly under pruning, so static
// partitioning would leave threads idle
template <typename RowBody>
void for_each_row_block(size_t n, unsigned num_threads, RowBody&& row_body) {
  if (num_threads <= 1) {
    for (size_t row = 0; row < n; ++row) {
      row_body(row);
    }
    return;
  }
  constexpr size_t kBlock = 32;
  std::atomic<size_t> next{0};
  const auto worker = [&] {
    while (true) {
      const size_t begin = next.fetch_add(1) * kBlock;
      if (begin >= n) {
        return;
      }
      const size_t end = std::min(n, begin + kBlock);
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
