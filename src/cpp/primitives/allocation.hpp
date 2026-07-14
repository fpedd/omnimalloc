//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <limits>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "buffer_kind.hpp"
#include "id_type.hpp"

namespace omnimalloc {

// A point in time: a scalar step on one global timeline, or a vector clock
// with one component per thread. 1-element vectors normalize to scalars.
// Must match TimePoint in src/python/omnimalloc/primitives/allocation.py
using TimePoint = std::variant<int64_t, std::vector<int64_t>>;

// Componentwise `end <= start`: the free at `end` happens-before (or
// coincides with) the alloc at `start`. Spans must have equal dimension.
[[nodiscard]] inline bool happens_before(
    std::span<const int64_t> end, std::span<const int64_t> start) noexcept {
  return std::ranges::equal(end, start, std::less_equal<int64_t>{});
}

class Allocation {
 public:
  Allocation(IdType id, int64_t size, TimePoint start, TimePoint end,
             std::optional<int64_t> offset = std::nullopt,
             std::optional<BufferKind> kind = std::nullopt);

  // Accessors
  const IdType& id() const noexcept { return id_; }
  int64_t size() const noexcept { return size_; }
  int64_t start() const { return scalar(start_); }  // throws on vector time
  int64_t end() const { return scalar(end_); }      // throws on vector time
  const TimePoint& start_time() const noexcept { return start_; }
  const TimePoint& end_time() const noexcept { return end_; }
  std::span<const int64_t> start_vec() const noexcept {
    return components(start_);
  }
  std::span<const int64_t> end_vec() const noexcept { return components(end_); }
  const std::optional<int64_t>& offset() const noexcept { return offset_; }
  const std::optional<BufferKind>& kind() const noexcept { return kind_; }

  // Computed properties
  bool is_allocated() const noexcept { return offset_.has_value(); }
  bool is_scalar_time() const noexcept {
    return std::holds_alternative<int64_t>(start_);
  }
  size_t dim() const noexcept { return start_vec().size(); }
  // L-inf: largest per-thread extent. Inline scalar fast path: called per
  // allocation per node in the supermalloc branch-and-bound heuristics.
  int64_t duration() const noexcept {
    if (is_scalar_time()) {
      return std::get<int64_t>(end_) - std::get<int64_t>(start_);
    }
    return vector_duration();
  }
  int64_t area() const noexcept {
    // Saturate instead of overflowing (UB) at int64 extremes
    const int64_t d = duration();
    if (d > 0 && size_ > std::numeric_limits<int64_t>::max() / d) {
      return std::numeric_limits<int64_t>::max();
    }
    return d * size_;
  }

  std::optional<int64_t> height() const noexcept {
    if (offset_.has_value()) {
      return offset_.value() + size_;
    }
    return std::nullopt;
  }

  // Overlap detection. Temporal overlap is the happens-before conflict test:
  // neither free happens-before the other's alloc. Dimension mismatch throws.
  bool overlaps_temporally(const Allocation& other) const;
  bool overlaps_spatially(const Allocation& other) const noexcept;
  bool overlaps(const Allocation& other) const;

  // Transformations
  Allocation with_offset(int64_t new_offset) const;
  Allocation with_kind(BufferKind new_kind) const;

  // Comparison
  bool operator==(const Allocation& other) const noexcept = default;

  // Stream output
  friend std::ostream& operator<<(std::ostream& os, const Allocation& a);

 private:
  IdType id_;
  int64_t size_;
  TimePoint start_;
  TimePoint end_;
  std::optional<int64_t> offset_;
  std::optional<BufferKind> kind_;

  // Inline fast path for the scalar accessors; the throw stays out-of-line
  static int64_t scalar(const TimePoint& time) {
    const auto* value = std::get_if<int64_t>(&time);
    if (value == nullptr) {
      throw_not_scalar(time);
    }
    return *value;
  }
  [[noreturn]] static void throw_not_scalar(const TimePoint& time);
  static std::span<const int64_t> components(const TimePoint& time) noexcept;
  int64_t vector_duration() const noexcept;
  void validate() const;
};

}  // namespace omnimalloc

namespace std {
template <>
struct hash<omnimalloc::Allocation> {
  size_t operator()(const omnimalloc::Allocation& a) const noexcept;
};
}  // namespace std
