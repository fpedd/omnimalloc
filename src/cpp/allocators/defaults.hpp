//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <chrono>
#include <optional>

namespace omnimalloc {

// Shared wall-clock budget for every time-bounded allocator (seconds).
// Mirrors DEFAULT_TIMEOUT in the Python package.
inline constexpr double kDefaultTimeout = 3.0;

// Deadline `timeout` seconds from now, or nullopt when `timeout` is not
// positive (the budget is disabled). The cap keeps the duration cast
// representable and also absorbs inf and NaN.
[[nodiscard]] inline std::optional<std::chrono::steady_clock::time_point>
make_deadline(double timeout) noexcept {
  if (timeout <= 0.0) {
    return std::nullopt;
  }
  constexpr double kMaxSeconds = 1e9;  // ~31 years
  const double seconds = timeout < kMaxSeconds ? timeout : kMaxSeconds;
  return std::chrono::steady_clock::now() +
         std::chrono::duration_cast<std::chrono::steady_clock::duration>(
             std::chrono::duration<double>(seconds));
}

// True when `deadline` is set and has passed.
[[nodiscard]] inline bool deadline_expired(
    const std::optional<std::chrono::steady_clock::time_point>&
        deadline) noexcept {
  return deadline && std::chrono::steady_clock::now() >= *deadline;
}

}  // namespace omnimalloc
