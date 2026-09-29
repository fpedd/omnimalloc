//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <string>
#include <variant>

namespace omnimalloc {

// Must match IdType in src/python/omnimalloc/primitives/allocation.py
using IdType = std::variant<int64_t, std::string>;

}  // namespace omnimalloc
