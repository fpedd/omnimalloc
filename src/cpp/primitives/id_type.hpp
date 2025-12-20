//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <typeindex>
#include <variant>

namespace omnimalloc {

using IdType = std::variant<int64_t, std::string>;

struct IdTypeHash {
  size_t operator()(const IdType& id) const noexcept {
    return std::visit(
        [](const auto& value) {
          using T = std::decay_t<decltype(value)>;
          return std::hash<T>{}(value) ^
                 std::hash<std::type_index>{}(typeid(T));
        },
        id);
  }
};

struct IdTypeEqual {
  bool operator()(const IdType& lhs, const IdType& rhs) const noexcept {
    return lhs == rhs;
  }
};

}  // namespace omnimalloc
