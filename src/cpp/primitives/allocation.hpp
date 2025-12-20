//
// SPDX-License-Identifier: Apache-2.0
//

#pragma once

#include <cstdint>
#include <iosfwd>
#include <optional>

#include "buffer_kind.hpp"
#include "id_type.hpp"

namespace omnimalloc {

class Allocation {
 public:
  Allocation(IdType id, int64_t size, int64_t start, int64_t end,
             std::optional<int64_t> offset = std::nullopt,
             std::optional<BufferKind> kind = std::nullopt);

  // Accessors
  const IdType& id() const noexcept { return id_; }
  int64_t size() const noexcept { return size_; }
  int64_t start() const noexcept { return start_; }
  int64_t end() const noexcept { return end_; }
  const std::optional<int64_t>& offset() const noexcept { return offset_; }
  const std::optional<BufferKind>& kind() const noexcept { return kind_; }

  // Computed properties
  bool is_allocated() const noexcept { return offset_.has_value(); }
  int64_t duration() const noexcept { return end_ - start_; }
  int64_t area() const noexcept { return duration() * size_; }

  std::optional<int64_t> height() const noexcept {
    if (offset_.has_value()) {
      return offset_.value() + size_;
    }
    return std::nullopt;
  }

  // Overlap detection
  bool overlaps_temporally(const Allocation& other) const noexcept;
  bool overlaps_spatially(const Allocation& other) const noexcept;
  bool overlaps(const Allocation& other) const noexcept;

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
  int64_t start_;
  int64_t end_;
  std::optional<int64_t> offset_;
  std::optional<BufferKind> kind_;

  void validate() const;
};

}  // namespace omnimalloc

namespace std {
template <>
struct hash<omnimalloc::Allocation> {
  size_t operator()(const omnimalloc::Allocation& a) const noexcept;
};
}  // namespace std
