//
// SPDX-License-Identifier: Apache-2.0
//

#include "allocation.hpp"

#include <algorithm>
#include <functional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "hash_utils.hpp"

namespace omnimalloc {

namespace {

TimePoint normalized(TimePoint time) {
  if (const auto* vec = std::get_if<std::vector<int64_t>>(&time);
      vec != nullptr && vec->size() == 1) {
    return vec->front();
  }
  return time;
}

std::string to_string(const TimePoint& time) {
  if (const auto* value = std::get_if<int64_t>(&time)) {
    return std::to_string(*value);
  }
  std::ostringstream ss;
  ss << '(';
  const char* separator = "";
  for (int64_t component : std::get<std::vector<int64_t>>(time)) {
    ss << separator << component;
    separator = ", ";
  }
  ss << ')';
  return ss.str();
}

}  // namespace

Allocation::Allocation(IdType id, int64_t size, TimePoint start, TimePoint end,
                       std::optional<int64_t> offset,
                       std::optional<BufferKind> kind)
    : id_(std::move(id)),
      size_(size),
      start_(normalized(std::move(start))),
      end_(normalized(std::move(end))),
      offset_(offset),
      kind_(kind) {
  validate();
}

void Allocation::throw_not_scalar(const TimePoint& time) {
  throw std::invalid_argument("scalar time accessor called on vector-time " +
                              to_string(time));
}

std::span<const int64_t> Allocation::components(
    const TimePoint& time) noexcept {
  if (const auto* value = std::get_if<int64_t>(&time)) {
    return {value, 1};
  }
  return std::get<std::vector<int64_t>>(time);
}

void Allocation::validate() const {
  if (size_ <= 0) {
    throw std::invalid_argument("size must be positive, got " +
                                std::to_string(size_));
  }
  if (dim() != end_vec().size()) {
    throw std::invalid_argument("start " + to_string(start_) + " and end " +
                                to_string(end_) +
                                " must share one clock dimension");
  }
  if (dim() == 0) {
    throw std::invalid_argument("time points must have at least one component");
  }
  if (is_scalar_time()) {
    if (start() < 0) {
      throw std::invalid_argument("start must be non-negative, got " +
                                  std::to_string(start()));
    }
    if (end() <= start()) {
      throw std::invalid_argument("end (" + std::to_string(end()) +
                                  ") must be > start (" +
                                  std::to_string(start()) + ")");
    }
  } else {
    if (std::ranges::any_of(start_vec(), [](int64_t c) { return c < 0; })) {
      throw std::invalid_argument(
          "start must be non-negative componentwise, got " + to_string(start_));
    }
    if (!happens_before(start_vec(), end_vec())) {
      throw std::invalid_argument("end " + to_string(end_) +
                                  " must be >= start " + to_string(start_) +
                                  " componentwise");
    }
    if (start_ == end_) {
      throw std::invalid_argument("end " + to_string(end_) +
                                  " must be > start " + to_string(start_) +
                                  " on at least one component");
    }
  }
  if (offset_.has_value() && offset_.value() < 0) {
    throw std::invalid_argument("offset must be non-negative, got " +
                                std::to_string(offset_.value()));
  }
}

int64_t Allocation::vector_duration() const noexcept {
  const auto start = start_vec();
  const auto end = end_vec();
  int64_t longest = 0;
  for (size_t i = 0; i < start.size(); ++i) {
    longest = std::max(longest, end[i] - start[i]);
  }
  return longest;
}

bool Allocation::overlaps_temporally(const Allocation& other) const {
  // Fast path: plain interval test, no variant probing in the O(n^2) callers
  if (is_scalar_time() && other.is_scalar_time()) {
    return std::get<int64_t>(start_) < std::get<int64_t>(other.end_) &&
           std::get<int64_t>(other.start_) < std::get<int64_t>(end_);
  }
  if (dim() != other.dim()) {
    throw std::invalid_argument(
        "clock dimension mismatch: " + std::to_string(dim()) + " vs " +
        std::to_string(other.dim()));
  }
  return !happens_before(end_vec(), other.start_vec()) &&
         !happens_before(other.end_vec(), start_vec());
}

bool Allocation::overlaps_spatially(const Allocation& other) const noexcept {
  return offset_.has_value() && other.offset_.has_value() &&
         offset_.value() < other.offset_.value() + other.size_ &&
         other.offset_.value() < offset_.value() + size_;
}

bool Allocation::overlaps(const Allocation& other) const {
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
  os << ", size=" << a.size_ << ", start=" << to_string(a.start_)
     << ", end=" << to_string(a.end_);
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
  const auto hash_time = [](std::span<const int64_t> components) {
    size_t seed = components.size();
    for (int64_t component : components) {
      seed = omnimalloc::make_hash(seed, component);
    }
    return seed;
  };
  const size_t id_hash = omnimalloc::IdTypeHash{}(a.id());
  const int64_t offset_val = a.offset().value_or(-1);
  const int kind_val =
      a.kind().has_value() ? static_cast<int>(a.kind().value()) : -1;
  return omnimalloc::make_hash(id_hash, a.size(), hash_time(a.start_vec()),
                               hash_time(a.end_vec()), offset_val, kind_val);
}

}  // namespace std
