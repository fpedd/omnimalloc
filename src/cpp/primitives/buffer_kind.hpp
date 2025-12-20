//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstddef>
#include <ostream>
#include <string_view>

namespace omnimalloc {

enum class BufferKind { WORKSPACE, CONSTANT, INPUT, OUTPUT };

constexpr bool is_io(BufferKind kind) noexcept {
  return kind == BufferKind::INPUT || kind == BufferKind::OUTPUT;
}

constexpr std::string_view to_string(BufferKind kind) noexcept {
  switch (kind) {
    case BufferKind::WORKSPACE:
      return "workspace";
    case BufferKind::CONSTANT:
      return "constant";
    case BufferKind::INPUT:
      return "input";
    case BufferKind::OUTPUT:
      return "output";
  }
  return "unknown";
}

inline std::ostream& operator<<(std::ostream& os, BufferKind kind) {
  return os << to_string(kind);
}

}  // namespace omnimalloc

namespace std {
template <>
struct hash<omnimalloc::BufferKind> {
  size_t operator()(omnimalloc::BufferKind kind) const noexcept {
    return hash<int>{}(static_cast<int>(kind));
  }
};
}  // namespace std
