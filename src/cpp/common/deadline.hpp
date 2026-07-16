//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <optional>
#include <stdexcept>
#include <string>

namespace omnimalloc {

// Deadline `timeout` seconds from now, or nullopt when `timeout` is nullopt
// (the budget is disabled). Throws on a non-positive or non-finite set
// `timeout`, so the boundary has exactly two states — positive budget or
// nullopt — even for raw-binding callers that bypass the Python-side
// validation. The cap keeps the duration cast representable.
[[nodiscard]] inline std::optional<std::chrono::steady_clock::time_point>
make_deadline(std::optional<double> timeout) {
  if (!timeout) {
    return std::nullopt;
  }
  if (!std::isfinite(*timeout) || *timeout <= 0.0) {
    throw std::invalid_argument("timeout must be positive or nullopt, got " +
                                std::to_string(*timeout) +
                                "; use nullopt (None) to disable the deadline");
  }
  constexpr double kMaxSeconds = 1e9;  // ~31 years
  const double seconds = std::min(*timeout, kMaxSeconds);
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
