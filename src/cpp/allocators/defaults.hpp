//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

namespace omnimalloc {

// Shared wall-clock budget for every time-bounded allocator (seconds).
// Mirrors DEFAULT_TIMEOUT in the Python package.
inline constexpr double kDefaultTimeout = 3.0;

}  // namespace omnimalloc
