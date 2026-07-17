//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstddef>
#include <ostream>
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

inline std::ostream& operator<<(std::ostream& os, AllocationKind kind) {
  return os << to_string(kind);
}

}  // namespace omnimalloc

namespace std {
template <>
struct hash<omnimalloc::AllocationKind> {
  size_t operator()(omnimalloc::AllocationKind kind) const noexcept {
    return hash<int>{}(static_cast<int>(kind));
  }
};
}  // namespace std
