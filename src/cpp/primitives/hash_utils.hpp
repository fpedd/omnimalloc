//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstddef>
#include <functional>

namespace omnimalloc {

// Generic hash combiner using boost::hash_combine algorithm
template <typename T, typename... Args>
[[nodiscard]] constexpr size_t make_hash(const T& first,
                                         const Args&... args) noexcept {
  size_t seed = std::hash<T>{}(first);
  if constexpr (sizeof...(args) > 0) {
    seed ^= make_hash(args...) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }
  return seed;
}

}  // namespace omnimalloc
