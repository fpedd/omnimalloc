//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>

namespace omnimalloc {

// Fold `value` into `seed` (boost's hash_combine with the 64-bit constant)
[[nodiscard]] constexpr uint64_t hash_combine(uint64_t seed,
                                              uint64_t value) noexcept {
  return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2));
}

}  // namespace omnimalloc
