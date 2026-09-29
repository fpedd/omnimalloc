//
// SPDX-License-Identifier: Apache-2.0
//

#include "allocation.hpp"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "common/hash.hpp"

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
                       std::optional<AllocationKind> kind)
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

void Allocation::validate() const {
  if (size_ <= 0) {
    throw std::invalid_argument("size must be positive, got " +
                                std::to_string(size_));
  }
  const auto start = start_vec();
  const auto end = end_vec();
  if (start.size() != end.size()) {
    throw std::invalid_argument("start " + to_string(start_) + " and end " +
                                to_string(end_) +
                                " must share one clock dimension");
  }
  if (start.empty()) {
    throw std::invalid_argument("time points must have at least one component");
  }
  const bool scalar = is_scalar_time();
  if (std::ranges::any_of(start, [](int64_t c) { return c < 0; })) {
    throw std::invalid_argument(std::string("start must be non-negative") +
                                (scalar ? "" : " componentwise") + ", got " +
                                to_string(start_));
  }
  if (!happens_before(start, end) || start_ == end_) {
    throw std::invalid_argument(
        "end " + to_string(end_) + " must be > start " + to_string(start_) +
        (scalar ? "" : " componentwise, strictly on at least one component"));
  }
  if (offset_.has_value() && offset_.value() < 0) {
    throw std::invalid_argument("offset must be non-negative, got " +
                                std::to_string(offset_.value()));
  }
  // height() and every downstream offset + size must stay in int64 (UB
  // otherwise)
  if (offset_.has_value() &&
      offset_.value() > std::numeric_limits<int64_t>::max() - size_) {
    throw std::invalid_argument("offset (" + std::to_string(offset_.value()) +
                                ") + size (" + std::to_string(size_) +
                                ") exceeds int64 range");
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

bool Allocation::conflicts_with(const Allocation& other) const {
  if (dim() != other.dim()) {
    throw std::invalid_argument(
        "allocations must share one clock dimension, got " +
        std::to_string(dim()) + " and " + std::to_string(other.dim()));
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
  return conflicts_with(other) && overlaps_spatially(other);
}

Allocation Allocation::with_offset(std::optional<int64_t> new_offset) const {
  return {id_, size_, start_, end_, new_offset, kind_};
}

std::ostream& operator<<(std::ostream& os, const Allocation& a) {
  os << "Allocation(id=";
  // Quote textual ids: unquoted, id=1 and id="1" would print alike
  std::visit(
      [&os](const auto& value) {
        if constexpr (std::is_same_v<std::decay_t<decltype(value)>,
                                     std::string>) {
          os << '\'' << value << '\'';
        } else {
          os << value;
        }
      },
      a.id_);
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
  uint64_t seed = hash<omnimalloc::IdType>{}(a.id());
  const auto mix = [&seed](auto value) {
    seed = omnimalloc::hash_combine(seed, static_cast<uint64_t>(value));
  };
  mix(a.size());
  for (const int64_t component : a.start_vec()) {
    mix(component);
  }
  for (const int64_t component : a.end_vec()) {
    mix(component);
  }
  mix(a.offset().value_or(-1));
  mix(a.kind().has_value() ? static_cast<int>(*a.kind()) : -1);
  return static_cast<size_t>(seed);
}

}  // namespace std
