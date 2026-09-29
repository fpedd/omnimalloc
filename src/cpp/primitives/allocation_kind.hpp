//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <string_view>

namespace omnimalloc {

enum class AllocationKind { WORKSPACE, CONSTANT, INPUT, OUTPUT };

[[nodiscard]] constexpr bool is_io(AllocationKind kind) noexcept {
  return kind == AllocationKind::INPUT || kind == AllocationKind::OUTPUT;
}

[[nodiscard]] constexpr std::string_view to_string(
    AllocationKind kind) noexcept {
  switch (kind) {
    case AllocationKind::WORKSPACE:
      return "workspace";
    case AllocationKind::CONSTANT:
      return "constant";
    case AllocationKind::INPUT:
      return "input";
    case AllocationKind::OUTPUT:
      return "output";
  }
  return "unknown";
}

}  // namespace omnimalloc
