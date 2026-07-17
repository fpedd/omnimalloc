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
#include <stdexcept>
#include <variant>
#include <vector>

#include "allocation_kind.hpp"
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
             std::optional<AllocationKind> kind = std::nullopt);

  // Accessors
  [[nodiscard]] const IdType& id() const noexcept { return id_; }
  [[nodiscard]] int64_t size() const noexcept { return size_; }
  // start()/end() throw on vector time
  [[nodiscard]] int64_t start() const { return scalar(start_); }
  [[nodiscard]] int64_t end() const { return scalar(end_); }
  [[nodiscard]] const TimePoint& start_time() const noexcept { return start_; }
  [[nodiscard]] const TimePoint& end_time() const noexcept { return end_; }
  [[nodiscard]] std::span<const int64_t> start_vec() const noexcept {
    return components(start_);
  }
  [[nodiscard]] std::span<const int64_t> end_vec() const noexcept {
    return components(end_);
  }
  [[nodiscard]] const std::optional<int64_t>& offset() const noexcept {
    return offset_;
  }
  [[nodiscard]] const std::optional<AllocationKind>& kind() const noexcept {
    return kind_;
  }

  // Computed properties
  [[nodiscard]] bool is_allocated() const noexcept {
    return offset_.has_value();
  }
  [[nodiscard]] bool is_scalar_time() const noexcept {
    return std::holds_alternative<int64_t>(start_);
  }
  [[nodiscard]] size_t dim() const noexcept { return start_vec().size(); }
  // L-inf: largest per-thread extent. Inline scalar fast path: called per
  // allocation per node in the supermalloc branch-and-bound heuristics.
  [[nodiscard]] int64_t duration() const noexcept {
    if (is_scalar_time()) {
      return std::get<int64_t>(end_) - std::get<int64_t>(start_);
    }
    return vector_duration();
  }
  [[nodiscard]] int64_t area() const noexcept {
    // Saturate instead of overflowing (UB) at int64 extremes
    const int64_t d = duration();
    if (d > 0 && size_ > std::numeric_limits<int64_t>::max() / d) {
      return std::numeric_limits<int64_t>::max();
    }
    return d * size_;
  }

  [[nodiscard]] std::optional<int64_t> height() const noexcept {
    if (offset_.has_value()) {
      return offset_.value() + size_;
    }
    return std::nullopt;
  }

  // Pair predicates. `conflicts_with` is the happens-before conflict test
  // (neither free happens-before the other's alloc; mixed clock dimensions
  // throw): conflicting allocations must occupy disjoint address ranges.
  // `overlaps` is the realized collision of two placed rectangles.
  [[nodiscard]] bool conflicts_with(const Allocation& other) const;
  [[nodiscard]] bool overlaps_spatially(const Allocation& other) const noexcept;
  [[nodiscard]] bool overlaps(const Allocation& other) const;

  // Transformations
  [[nodiscard]] Allocation with_offset(int64_t new_offset) const;

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
  std::optional<AllocationKind> kind_;

  // Inline fast path for the scalar accessors; the throw stays out-of-line
  static int64_t scalar(const TimePoint& time) {
    const auto* value = std::get_if<int64_t>(&time);
    if (value == nullptr) {
      throw_not_scalar(time);
    }
    return *value;
  }
  [[noreturn]] static void throw_not_scalar(const TimePoint& time);
  // Inline: called per comparison in the clock-row sort/dedupe hot loops
  static std::span<const int64_t> components(const TimePoint& time) noexcept {
    if (const auto* value = std::get_if<int64_t>(&time)) {
      return {value, 1};
    }
    return std::get<std::vector<int64_t>>(time);
  }
  int64_t vector_duration() const noexcept;
  void validate() const;
};

// Throw std::invalid_argument (a precondition on the input sizes) when the
// total allocation size exceeds `limit`, ruling out signed overflow in
// downstream sums of sizes: placer offset and cursor arithmetic, sweep
// deltas, and flow arc capacities. The `limit` default is an internal
// correctness bound, not a policy value crossing the binding surface.
inline void check_total_size(
    const std::vector<Allocation>& allocations,
    int64_t limit = std::numeric_limits<int64_t>::max() / 2) {
  int64_t total_size = 0;
  for (const Allocation& a : allocations) {
    if (a.size() > limit - total_size) {
      throw std::invalid_argument("Total allocation size exceeds int64 range");
    }
    total_size += a.size();
  }
}

}  // namespace omnimalloc

namespace std {
template <>
struct hash<omnimalloc::Allocation> {
  size_t operator()(const omnimalloc::Allocation& a) const noexcept;
};
}  // namespace std
