//
// SPDX-License-Identifier: Apache-2.0
//

#include "allocation.hpp"

#include <ostream>
#include <stdexcept>

#include "hash_utils.hpp"

namespace omnimalloc {

Allocation::Allocation(IdType id, int64_t size, int64_t start, int64_t end,
                       std::optional<int64_t> offset,
                       std::optional<BufferKind> kind)
    : id_(std::move(id)),
      size_(size),
      start_(start),
      end_(end),
      offset_(offset),
      kind_(kind) {
  validate();
}

void Allocation::validate() const {
  if (size_ <= 0) {
    throw std::invalid_argument("size must be positive, got " +
                                std::to_string(size_));
  }
  if (start_ < 0) {
    throw std::invalid_argument("start must be non-negative, got " +
                                std::to_string(start_));
  }
  if (end_ <= start_) {
    throw std::invalid_argument("end (" + std::to_string(end_) +
                                ") must be > start (" + std::to_string(start_) +
                                ")");
  }
  if (offset_.has_value() && offset_.value() < 0) {
    throw std::invalid_argument("offset must be non-negative, got " +
                                std::to_string(offset_.value()));
  }
}

bool Allocation::overlaps_temporally(const Allocation& other) const noexcept {
  return start_ < other.end_ && other.start_ < end_;
}

bool Allocation::overlaps_spatially(const Allocation& other) const noexcept {
  return offset_.has_value() && other.offset_.has_value() &&
         offset_.value() < other.offset_.value() + other.size_ &&
         other.offset_.value() < offset_.value() + size_;
}

bool Allocation::overlaps(const Allocation& other) const noexcept {
  return overlaps_temporally(other) && overlaps_spatially(other);
}

Allocation Allocation::with_offset(int64_t new_offset) const {
  return {id_, size_, start_, end_, new_offset, kind_};
}

Allocation Allocation::with_kind(BufferKind new_kind) const {
  return {id_, size_, start_, end_, offset_, new_kind};
}

std::ostream& operator<<(std::ostream& os, const Allocation& a) {
  os << "Allocation(id=";
  std::visit([&os](const auto& value) { os << value; }, a.id_);
  os << ", size=" << a.size_ << ", start=" << a.start_ << ", end=" << a.end_;
  if (a.offset_.has_value()) {
    os << ", offset=" << a.offset_.value();
  }
  if (a.kind_.has_value()) {
    os << ", kind=" << to_string(a.kind_.value());
  }
  return os << ')';
}

}  // namespace omnimalloc

namespace std {

size_t hash<omnimalloc::Allocation>::operator()(
    const omnimalloc::Allocation& a) const noexcept {
  const size_t id_hash = omnimalloc::IdTypeHash{}(a.id());
  const int64_t offset_val = a.offset().value_or(-1);
  const int kind_val =
      a.kind().has_value() ? static_cast<int>(a.kind().value()) : -1;
  return omnimalloc::make_hash(id_hash, a.size(), a.start(), a.end(),
                               offset_val, kind_val);
}

}  // namespace std
