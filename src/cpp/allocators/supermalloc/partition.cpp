//
// SPDX-License-Identifier: Apache-2.0
//

#include "partition.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <iterator>
#include <mutex>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "allocators/defaults.hpp"
#include "allocators/first_fit.hpp"

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX  // keep <windows.h> from defining min/max macros
#endif
#include <process.h>
#include <windows.h>
#else
#include <pthread.h>
#endif

namespace omnimalloc {

namespace {

constexpr int kExitEvent = 0;
constexpr int kEnterEvent = 1;

constexpr std::string_view kSortKeys = "ACLOTUWZ";

// One sort key for `code`, negated so an ascending sort yields descending
// values. `span_len`, `overlap_count`, and `max_total` are precomputed by the
// caller over its section band.
int64_t heuristic_key(char code, const Allocation& a, int64_t span_len,
                      int64_t overlap_count, int64_t max_total) {
  switch (code) {
    case 'A': {
      // Saturate instead of overflowing; both factors are positive.
      const int64_t duration = a.duration();
      const int64_t size = a.size();
      return duration > INT64_MAX / size ? INT64_MIN + 1 : -(duration * size);
    }
    case 'C':
      return -span_len;
    case 'L':
      return -a.start();
    case 'O':
      return -overlap_count;
    case 'T':
      return -max_total;
    case 'U':
      return -a.end();
    case 'W':
      return -a.duration();
    case 'Z':
      return -a.size();
    default:
      throw std::invalid_argument("Unknown sort key: " + std::string(1, code));
  }
}

// Reject unknown sort-key characters up front, on the calling thread;
// heuristics are otherwise parsed lazily inside the worker threads.
void validate_heuristics(const std::vector<std::string>& heuristics) {
  for (const std::string& h : heuristics) {
    for (char c : h) {
      if (kSortKeys.find(c) == std::string_view::npos) {
        throw std::invalid_argument("Unknown sort key: " + std::string(1, c));
      }
    }
  }
}

// For each allocation, the largest lower index with identical start, end, and
// size, or -1. Such buffers are fully interchangeable, so the search may
// force them into index order (symmetry breaking).
std::vector<int> compute_sym_predecessor(
    const std::vector<Allocation>& allocations) {
  const int n = static_cast<int>(allocations.size());
  std::vector<std::pair<std::tuple<int64_t, int64_t, int64_t>, int>> keyed;
  keyed.reserve(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    const Allocation& a = allocations[i];
    keyed.emplace_back(std::make_tuple(a.start(), a.end(), a.size()), i);
  }
  std::sort(keyed.begin(), keyed.end());

  std::vector<int> pred(static_cast<size_t>(n), -1);
  for (int k = 1; k < n; ++k) {
    if (keyed[k].first == keyed[k - 1].first) {
      pred[keyed[k].second] = keyed[k - 1].second;
    }
  }
  return pred;
}

}  // namespace

std::shared_ptr<Partition::SharedData> Partition::make_shared_data(
    std::vector<Allocation> allocations, std::vector<int64_t> alloc_sizes,
    std::vector<std::vector<int>> sections,
    std::vector<std::vector<int>> overlaps,
    std::vector<std::pair<int, int>> section_spans) {
  std::vector<int> sym_predecessor = compute_sym_predecessor(allocations);
  return std::make_shared<SharedData>(SharedData{
      std::move(allocations),
      std::move(alloc_sizes),
      std::move(sections),
      std::move(overlaps),
      std::move(section_spans),
      std::move(sym_predecessor),
  });
}

std::shared_ptr<Partition::SharedData> Partition::build_shared_data(
    std::vector<Allocation> allocations) {
  const int n = static_cast<int>(allocations.size());

  // Tuple ordering puts EXIT (0) before ENTER (1) at equal timestamps.
  std::vector<std::tuple<int64_t, int, int>> events;
  events.reserve(static_cast<size_t>(n) * 2);
  for (int i = 0; i < n; ++i) {
    events.emplace_back(allocations[i].start(), kEnterEvent, i);
    events.emplace_back(allocations[i].end(), kExitEvent, i);
  }
  std::sort(events.begin(), events.end());

  std::vector<std::vector<int>> sections;
  std::vector<std::vector<int>> overlaps(static_cast<size_t>(n));
  std::vector<std::pair<int, int>> section_spans(static_cast<size_t>(n));

  std::unordered_set<int> alive;

  bool has_prev = false;
  int64_t prev_time = 0;
  int prev_event = 0;

  for (const auto& [time, event, idx] : events) {
    // A section is a maximal span over which the alive set is constant, so
    // snapshot it (when non-empty) right before it changes.
    const bool time_changed = !has_prev || time != prev_time;
    const bool exit_to_enter =
        has_prev && prev_event == kExitEvent && event == kEnterEvent;
    if (!alive.empty() && (time_changed || exit_to_enter)) {
      sections.emplace_back(alive.begin(), alive.end());
    }

    has_prev = true;
    prev_time = time;
    prev_event = event;

    if (event == kExitEvent) {
      section_spans[idx].second = static_cast<int>(sections.size());
      alive.erase(idx);
    } else {
      for (int other : alive) {
        overlaps[idx].push_back(other);
        overlaps[other].push_back(idx);
      }
      section_spans[idx].first = static_cast<int>(sections.size());
      alive.insert(idx);
    }
  }

  std::vector<int64_t> alloc_sizes(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) alloc_sizes[i] = allocations[i].size();

  return make_shared_data(std::move(allocations), std::move(alloc_sizes),
                          std::move(sections), std::move(overlaps),
                          std::move(section_spans));
}

Partition::Partition(std::shared_ptr<const SharedData> data,
                     std::vector<int64_t> min_offsets,
                     std::vector<int64_t> section_floors,
                     std::vector<int64_t> section_totals,
                     std::vector<int64_t> offsets, int64_t best_height)
    : data_(std::move(data)),
      min_offsets_(std::move(min_offsets)),
      section_floors_(std::move(section_floors)),
      section_totals_(std::move(section_totals)),
      offsets_(std::move(offsets)),
      best_height_(best_height) {
  init_search_state();
}

void Partition::init_search_state() {
  const int n = static_cast<int>(data_->allocations.size());
  const size_t num_sections = data_->sections.size();
  cuts_.assign(num_sections > 0 ? num_sections - 1 : 0, 0);
  affected_scratch_.assign(num_sections, 0);
  for (int i = 0; i < n; ++i) {
    if (offsets_[i] >= 0) continue;
    candidates_.emplace(min_offsets_[i], i);
    tops_.insert(min_offsets_[i] + data_->alloc_sizes[i]);
    const auto [first, last] = data_->section_spans[i];
    for (int b = first; b < last - 1; ++b) cuts_[b] += 1;
  }
  num_zero_cuts_ = 0;
  for (int64_t c : cuts_) num_zero_cuts_ += static_cast<int>(c == 0);
}

Partition Partition::from_allocations(std::vector<Allocation> allocations) {
  // The section grid needs a linear timeline; reject vector clocks here.
  if (!std::ranges::all_of(allocations, &Allocation::is_scalar_time)) {
    throw std::invalid_argument(
        "Partition requires scalar time lifetimes; linearize vector clocks "
        "first");
  }
  // Every offset, top, and floor + total the search computes is bounded by
  // twice the total size, so rejecting sums above INT64_MAX / 2 rules out
  // signed overflow everywhere downstream.
  check_total_size(allocations);

  auto data = build_shared_data(std::move(allocations));

  const int n = static_cast<int>(data->allocations.size());
  const size_t num_sections = data->sections.size();
  std::vector<int64_t> section_totals(num_sections, 0);
  for (size_t s = 0; s < num_sections; ++s) {
    for (int idx : data->sections[s]) {
      section_totals[s] += data->alloc_sizes[idx];
    }
  }

  std::vector<int64_t> min_offsets(static_cast<size_t>(n), 0);
  std::vector<int64_t> section_floors(num_sections, 0);
  std::vector<int64_t> offsets(static_cast<size_t>(n), -1);

  return Partition(std::move(data), std::move(min_offsets),
                   std::move(section_floors), std::move(section_totals),
                   std::move(offsets), INT64_MAX);
}

int64_t Partition::height() const noexcept {
  int64_t h = 0;
  const int n = static_cast<int>(data_->allocations.size());
  for (int i = 0; i < n; ++i) {
    if (offsets_[i] >= 0) h = std::max(h, offsets_[i] + data_->alloc_sizes[i]);
  }
  return h;
}

int64_t Partition::min_height() const noexcept {
  return tops_.empty() ? INT64_MAX : *tops_.begin();
}

int64_t Partition::lower_bound() const noexcept { return scan_bounds().first; }

std::pair<int64_t, int64_t> Partition::scan_bounds() const noexcept {
  int64_t lb = 0;
  int64_t max_total = 0;
  for (size_t s = 0; s < section_floors_.size(); ++s) {
    lb = std::max(lb, section_floors_[s] + section_totals_[s]);
    max_total = std::max(max_total, section_totals_[s]);
  }
  return {lb, max_total};
}

bool Partition::can_allocate_at(int idx, bool monotonic_floor,
                                int64_t lower_bound,
                                int64_t max_total) const noexcept {
  // Spanned sections take the `top` bump and shed `size`; with
  // `monotonic_floor`, every other section's floor is raised to the placement
  // offset. Floors are never negative, so a `floor_min` of 0 is a no-op.
  const int64_t offset = min_offsets_[idx];
  const int64_t alloc_size = data_->alloc_sizes[idx];
  const auto [first, last] = data_->section_spans[idx];
  const int64_t top = offset + alloc_size;
  const int num_sections = static_cast<int>(data_->sections.size());
  const int64_t floor_min = monotonic_floor ? offset : 0;

  for (int s = first; s < last; ++s) {
    if (std::max(section_floors_[s], top) + section_totals_[s] - alloc_size >=
        best_height_) {
      return false;
    }
  }

  // Screen: every unspanned section's max(floor_s, floor_min) + total_s is
  // bounded by max(lower_bound, floor_min + max_total), so when that bound
  // fits, the exact scan cannot fail.
  if (lower_bound < best_height_ && floor_min + max_total < best_height_) {
    return true;
  }

  const auto unspanned_fit = [&](int lo, int hi) noexcept {
    for (int s = lo; s < hi; ++s) {
      if (std::max(section_floors_[s], floor_min) + section_totals_[s] >=
          best_height_) {
        return false;
      }
    }
    return true;
  };
  return unspanned_fit(0, first) && unspanned_fit(last, num_sections);
}

bool Partition::placement_feasible(const PlacementUndo& undo, int64_t offset,
                                   bool monotonic_floor) const noexcept {
  // With `monotonic_floor`, the parent's placement `offset` acts as a floor
  // for every section. On the sections the placement spans, this expression
  // equals the spanned check `can_allocate_at` already passed, so it adds no
  // false rejections there.
  const int64_t floor_min = monotonic_floor ? offset : 0;
  for (const auto& [s, old_floor] : undo.floor_changes) {
    if (std::max(section_floors_[s], floor_min) + section_totals_[s] >=
        best_height_) {
      return false;
    }
  }
  return true;
}

const Partition::PlacementUndo& Partition::apply_at(int idx,
                                                    bool floor_inference) {
  const int64_t offset = min_offsets_[idx];
  const int64_t alloc_size = data_->alloc_sizes[idx];
  const int64_t top = offset + alloc_size;
  const auto [first, last] = data_->section_spans[idx];

  if (undo_depth_ == undo_pool_.size()) undo_pool_.emplace_back();
  PlacementUndo& undo = undo_pool_[undo_depth_++];
  undo.idx = idx;
  undo.floor_changes.clear();
  undo.min_offset_changes.clear();

  offsets_[idx] = offset;
  candidates_.erase({offset, idx});
  tops_.erase(tops_.find(top));

  for (int s = first; s < last; ++s) {
    undo.floor_changes.emplace_back(s, section_floors_[s]);
    section_floors_[s] = std::max(section_floors_[s], top);
    section_totals_[s] -= alloc_size;
  }
  for (int b = first; b < last - 1; ++b) {
    if (--cuts_[b] == 0) ++num_zero_cuts_;
  }

  // Propagate the placed allocation's top to overlapping unplaced
  // allocations, tracking affected sections for the floor inference below.
  // Extract/insert node handles reuse the tree nodes instead of freeing and
  // reallocating one per update.
  for (int j : data_->overlaps[idx]) {
    if (offsets_[j] >= 0) continue;
    const int64_t old_min = min_offsets_[j];
    if (top <= old_min) continue;
    undo.min_offset_changes.emplace_back(j, old_min);
    min_offsets_[j] = top;
    auto candidate = candidates_.extract({old_min, j});
    candidate.value() = {top, j};
    candidates_.insert(std::move(candidate));
    const int64_t j_size = data_->alloc_sizes[j];
    auto top_node = tops_.extract(tops_.find(old_min + j_size));
    top_node.value() = top + j_size;
    tops_.insert(std::move(top_node));
    if (!floor_inference) continue;
    const auto [jf, jl] = data_->section_spans[j];
    for (int s = jf; s < jl; ++s) {
      if (!affected_scratch_[s]) {
        affected_scratch_[s] = 1;
        touched_sections_.push_back(s);
      }
    }
  }

  // Raise section floors where every remaining unplaced allocation has been
  // pushed above the current floor. Sections in [first, last) host the
  // just-placed allocation, so their floor cannot be raised further.
  for (int s : touched_sections_) {
    affected_scratch_[s] = 0;
    if (first <= s && s < last) continue;
    const int64_t floor_s = section_floors_[s];
    int64_t s_min = INT64_MAX;
    bool floor_pinned = false;
    for (int b : data_->sections[s]) {
      if (offsets_[b] >= 0) continue;
      const int64_t off = min_offsets_[b];
      if (off <= floor_s) {
        floor_pinned = true;
        break;
      }
      if (off < s_min) s_min = off;
    }
    if (!floor_pinned && s_min < INT64_MAX) {
      undo.floor_changes.emplace_back(s, section_floors_[s]);
      section_floors_[s] = s_min;
    }
  }
  touched_sections_.clear();

  return undo;
}

void Partition::revert(const PlacementUndo& undo) {
  const auto [first, last] = data_->section_spans[undo.idx];
  const int64_t alloc_size = data_->alloc_sizes[undo.idx];
  for (const auto& [s, old_floor] : undo.floor_changes) {
    section_floors_[s] = old_floor;
  }
  for (const auto& [j, old_min] : undo.min_offset_changes) {
    const int64_t j_size = data_->alloc_sizes[j];
    auto candidate = candidates_.extract({min_offsets_[j], j});
    candidate.value() = {old_min, j};
    candidates_.insert(std::move(candidate));
    auto top_node = tops_.extract(tops_.find(min_offsets_[j] + j_size));
    top_node.value() = old_min + j_size;
    tops_.insert(std::move(top_node));
    min_offsets_[j] = old_min;
  }
  for (int s = first; s < last; ++s) {
    section_totals_[s] += alloc_size;
  }
  for (int b = first; b < last - 1; ++b) {
    if (cuts_[b]++ == 0) --num_zero_cuts_;
  }
  offsets_[undo.idx] = -1;
  candidates_.emplace(min_offsets_[undo.idx], undo.idx);
  tops_.insert(min_offsets_[undo.idx] + alloc_size);
  --undo_depth_;
}

void Partition::order_indices(std::vector<int>& indices,
                              const std::string& heuristic, int start,
                              int end) const {
  const int n = static_cast<int>(data_->allocations.size());
  const int m = static_cast<int>(indices.size());
  const size_t key_len = heuristic.size() + 1;

  std::vector<char> member(static_cast<size_t>(n), 0);
  for (int idx : indices) member[idx] = 1;

  // Per-allocation sort keys, row-major in one flat buffer; the original
  // index is the final tiebreaker.
  std::vector<int64_t> keys(static_cast<size_t>(m) * key_len);
  for (int i = 0; i < m; ++i) {
    const int idx = indices[i];
    int64_t* row = keys.data() + static_cast<size_t>(i) * key_len;
    const Allocation& a = data_->allocations[idx];
    const auto [first, last] = data_->section_spans[idx];
    const int lo = std::max(first, start);
    const int hi = std::min(last, end);
    int64_t overlap_count = 0;
    for (int j : data_->overlaps[idx]) overlap_count += member[j];
    int64_t max_total = 0;
    for (int s = lo; s < hi; ++s) {
      if (section_totals_[s] > max_total) max_total = section_totals_[s];
    }
    for (size_t k = 0; k < heuristic.size(); ++k) {
      row[k] =
          heuristic_key(heuristic[k], a, hi - lo, overlap_count, max_total);
    }
    row[key_len - 1] = idx;
  }

  std::vector<int> pos(static_cast<size_t>(m));
  std::iota(pos.begin(), pos.end(), 0);
  std::sort(pos.begin(), pos.end(), [&](int a, int b) {
    const int64_t* ra = keys.data() + static_cast<size_t>(a) * key_len;
    const int64_t* rb = keys.data() + static_cast<size_t>(b) * key_len;
    return std::lexicographical_compare(ra, ra + key_len, rb, rb + key_len);
  });

  std::vector<int> sorted(static_cast<size_t>(m));
  for (int i = 0; i < m; ++i) sorted[i] = indices[pos[i]];
  indices = std::move(sorted);
}

Solution Partition::first_fit(const std::vector<int>& order) const {
  const int n = static_cast<int>(data_->allocations.size());
  std::vector<int64_t> offsets(static_cast<size_t>(n), -1);
  int64_t height = 0;
  std::vector<std::pair<int64_t, int64_t>> intervals;
  for (int i : order) {
    intervals.clear();
    for (int j : data_->overlaps[i]) {
      if (offsets[j] >= 0) {
        intervals.emplace_back(offsets[j], offsets[j] + data_->alloc_sizes[j]);
      }
    }
    std::sort(intervals.begin(), intervals.end());

    const int64_t size = data_->alloc_sizes[i];
    int64_t offset = 0;
    for (const auto& [lo, hi] : intervals) {
      if (lo - offset >= size) break;
      offset = std::max(offset, hi);
    }
    offsets[i] = offset;
    height = std::max(height, offset + size);
  }
  return Solution{data_->allocations, std::move(offsets), height};
}

Solution Partition::greedy_pack(const std::string& heuristic) const {
  std::vector<int> order(data_->allocations.size());
  std::iota(order.begin(), order.end(), 0);
  order_indices(order, heuristic, 0, static_cast<int>(data_->sections.size()));
  return first_fit(order);
}

Partition Partition::reorder(const std::string& heuristic) const {
  const int n = static_cast<int>(data_->allocations.size());
  std::vector<int> order(static_cast<size_t>(n));
  std::iota(order.begin(), order.end(), 0);
  order_indices(order, heuristic, 0, static_cast<int>(data_->sections.size()));

  // Inverse permutation: old index -> new index. Renumbering every
  // index-bearing array leaves the section grid (floors, totals) untouched.
  std::vector<int> inv(static_cast<size_t>(n));
  for (int new_idx = 0; new_idx < n; ++new_idx) inv[order[new_idx]] = new_idx;

  std::vector<Allocation> new_allocations;
  std::vector<int64_t> new_alloc_sizes(static_cast<size_t>(n));
  std::vector<std::pair<int, int>> new_section_spans(static_cast<size_t>(n));
  std::vector<std::vector<int>> new_overlaps(static_cast<size_t>(n));
  std::vector<int64_t> new_min_offsets(static_cast<size_t>(n));
  std::vector<int64_t> new_offsets(static_cast<size_t>(n));
  new_allocations.reserve(static_cast<size_t>(n));
  for (int new_idx = 0; new_idx < n; ++new_idx) {
    const int old_idx = order[new_idx];
    new_allocations.push_back(data_->allocations[old_idx]);
    new_alloc_sizes[new_idx] = data_->alloc_sizes[old_idx];
    new_section_spans[new_idx] = data_->section_spans[old_idx];
    new_min_offsets[new_idx] = min_offsets_[old_idx];
    new_offsets[new_idx] = offsets_[old_idx];
    for (int j : data_->overlaps[old_idx])
      new_overlaps[new_idx].push_back(inv[j]);
  }

  std::vector<std::vector<int>> new_sections(data_->sections.size());
  for (size_t s = 0; s < data_->sections.size(); ++s) {
    for (int idx : data_->sections[s]) new_sections[s].push_back(inv[idx]);
  }

  auto new_data =
      make_shared_data(std::move(new_allocations), std::move(new_alloc_sizes),
                       std::move(new_sections), std::move(new_overlaps),
                       std::move(new_section_spans));

  Partition reordered(std::move(new_data), std::move(new_min_offsets),
                      section_floors_, section_totals_, std::move(new_offsets),
                      best_height_);
  reordered.heuristic_ = heuristic;
  return reordered;
}

std::optional<Partition> Partition::build_sub_partition(int start,
                                                        int end) const {
  const int n = static_cast<int>(data_->allocations.size());

  // A placed buffer may still straddle a zero-cut boundary; assign it to the
  // band holding its first section so the sub-parts stay disjoint for merge.
  // Its height still constrains the other bands via the carried floors.
  std::vector<int> sub_old_indices;
  sub_old_indices.reserve(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    const auto [first, last] = data_->section_spans[i];
    const bool placed = offsets_[i] >= 0;
    const bool include = placed ? (start <= first && first < end)
                                : (first < end && last > start);
    if (include) sub_old_indices.push_back(i);
  }
  if (sub_old_indices.empty()) return std::nullopt;

  // Keys computed from the parent's data clamped to the band equal the
  // sub-part's own, so this matches a build-then-reorder exactly.
  if (!heuristic_.empty()) {
    order_indices(sub_old_indices, heuristic_, start, end);
  }

  const int sub_n = static_cast<int>(sub_old_indices.size());
  const int sub_num_sections = end - start;

  std::vector<int> old_to_new(static_cast<size_t>(n), -1);
  std::vector<Allocation> sub_allocs;
  sub_allocs.reserve(static_cast<size_t>(sub_n));
  std::vector<int64_t> sub_alloc_sizes;
  sub_alloc_sizes.reserve(static_cast<size_t>(sub_n));
  for (int new_idx = 0; new_idx < sub_n; ++new_idx) {
    const int old_idx = sub_old_indices[new_idx];
    old_to_new[old_idx] = new_idx;
    sub_allocs.push_back(data_->allocations[old_idx]);
    sub_alloc_sizes.push_back(data_->alloc_sizes[old_idx]);
  }

  std::vector<std::pair<int, int>> sub_section_spans(
      static_cast<size_t>(sub_n));
  std::vector<int64_t> sub_min_offsets(static_cast<size_t>(sub_n));
  std::vector<int64_t> sub_offsets(static_cast<size_t>(sub_n));
  for (int new_idx = 0; new_idx < sub_n; ++new_idx) {
    const int old_idx = sub_old_indices[new_idx];
    const auto [first, last] = data_->section_spans[old_idx];
    sub_section_spans[new_idx] = {std::max(first, start) - start,
                                  std::min(last, end) - start};
    sub_min_offsets[new_idx] = min_offsets_[old_idx];
    sub_offsets[new_idx] = offsets_[old_idx];
  }

  std::vector<std::vector<int>> sub_sections(
      static_cast<size_t>(sub_num_sections));
  for (int s = start; s < end; ++s) {
    auto& bucket = sub_sections[s - start];
    for (int idx : data_->sections[s]) {
      if (old_to_new[idx] >= 0) bucket.push_back(old_to_new[idx]);
    }
  }

  std::vector<std::vector<int>> sub_overlaps(static_cast<size_t>(sub_n));
  for (int new_idx = 0; new_idx < sub_n; ++new_idx) {
    const int old_idx = sub_old_indices[new_idx];
    auto& bucket = sub_overlaps[new_idx];
    for (int j_old : data_->overlaps[old_idx]) {
      if (old_to_new[j_old] >= 0) bucket.push_back(old_to_new[j_old]);
    }
  }

  std::vector<int64_t> sub_section_floors(section_floors_.begin() + start,
                                          section_floors_.begin() + end);
  std::vector<int64_t> sub_section_totals(section_totals_.begin() + start,
                                          section_totals_.begin() + end);

  auto sub_data =
      make_shared_data(std::move(sub_allocs), std::move(sub_alloc_sizes),
                       std::move(sub_sections), std::move(sub_overlaps),
                       std::move(sub_section_spans));

  Partition sub(std::move(sub_data), std::move(sub_min_offsets),
                std::move(sub_section_floors), std::move(sub_section_totals),
                std::move(sub_offsets), best_height_);
  sub.heuristic_ = heuristic_;
  return sub;
}

std::optional<std::vector<Partition>> Partition::decompose() const {
  if (num_zero_cuts_ == 0) return std::nullopt;

  std::vector<int> boundaries{0};
  for (size_t b = 0; b < cuts_.size(); ++b) {
    if (cuts_[b] == 0) boundaries.push_back(static_cast<int>(b + 1));
  }
  boundaries.push_back(static_cast<int>(data_->sections.size()));

  std::vector<Partition> sub_parts;
  for (size_t b = 0; b + 1 < boundaries.size(); ++b) {
    std::optional<Partition> sub =
        build_sub_partition(boundaries[b], boundaries[b + 1]);
    if (sub) sub_parts.push_back(std::move(*sub));
  }
  return sub_parts;
}

namespace {

// Lock-free minimum: lower `shared` to `h` if it is smaller.
void lower_shared(std::atomic<int64_t>* shared, int64_t h) {
  if (!shared) return;
  int64_t cur = shared->load(std::memory_order_relaxed);
  while (h < cur && !shared->compare_exchange_weak(cur, h)) {
  }
}

// Pull in any improvement a sibling portfolio thread published.
void pull_shared(const std::atomic<int64_t>* shared, Partition& node) {
  if (!shared) return;
  const int64_t gb = shared->load(std::memory_order_relaxed);
  if (gb < node.best_height()) node.set_best_height(gb);
}

// Run-wide search invariants plus the node counter.
struct SearchCtx {
  int64_t node_limit;
  std::chrono::steady_clock::time_point deadline;
  SearchOptions opts;
  // The portfolio-wide best bound and the problem's lower bound: once the
  // bound reaches the lower bound, the whole portfolio is done. Checked on
  // every node so even sub-part solves (which otherwise never read the
  // shared bound) stop promptly.
  const std::atomic<int64_t>* portfolio_best;
  int64_t problem_lower_bound;
  int64_t nodes = 0;
  // Set when a limit is hit or the portfolio finished; makes every frame on
  // the recursion stack abandon its candidate loop instead of probing the
  // remaining candidates on the way out.
  bool stopped = false;
};

// Concatenate sub-part solutions; the sub-parts are disjoint by construction.
Solution merge_solutions(std::vector<Solution> subs, int64_t height) {
  size_t total = 0;
  for (const Solution& s : subs) total += s.allocations.size();

  Solution merged{{}, {}, height};
  merged.allocations.reserve(total);
  merged.offsets.reserve(total);
  for (Solution& s : subs) {
    std::move(s.allocations.begin(), s.allocations.end(),
              std::back_inserter(merged.allocations));
    merged.offsets.insert(merged.offsets.end(), s.offsets.begin(),
                          s.offsets.end());
  }
  return merged;
}

void solve_dfs(Partition& node, int64_t min_offset, int min_idx, SearchCtx& ctx,
               std::optional<Solution>& best, std::atomic<int64_t>* shared);

// Ratchet a decomposition: solve every sub-part below the node's bound, then
// keep re-solving the bottleneck sub-parts under each merged height until one
// proves infeasible. Publishes every improvement, so a decomposable problem
// optimizes anytime within a single search. Sub-part solves pass a null
// `shared` since their partial-problem heights must never reach the portfolio
// bound; only merged results publish.
void solve_decomposed(std::vector<Partition>& sub_parts, Partition& node,
                      SearchCtx& ctx, std::optional<Solution>& best,
                      std::atomic<int64_t>* shared) {
  const size_t count = sub_parts.size();
  std::vector<Solution> sub_solutions(count);
  std::vector<int64_t> sub_heights(count, INT64_MAX);

  while (!ctx.stopped) {
    pull_shared(shared, node);

    // Re-solve only the sub-parts at or above the bound; the rest keep their
    // packing from earlier rounds.
    const int64_t bound = node.best_height();
    int64_t merged_height = 0;
    for (size_t i = 0; i < count; ++i) {
      if (sub_heights[i] >= bound) {
        sub_parts[i].set_best_height(bound);
        std::optional<Solution> sub_best;
        solve_dfs(sub_parts[i], 0, 0, ctx, sub_best, nullptr);
        if (!sub_best) return;  // no packing below the bound: merge is final
        sub_heights[i] = sub_best->height;
        sub_solutions[i] = std::move(*sub_best);
      }
      merged_height = std::max(merged_height, sub_heights[i]);
    }

    node.set_best_height(merged_height);
    best = merge_solutions(sub_solutions, merged_height);
    lower_shared(shared, merged_height);
    // A sub-part only has to fit the inherited bound, not reach its own
    // optimum.
    if (shared == nullptr) return;
  }
}

// Recursive branch-and-bound descent. `node` is mutated in place via
// `apply_at`/`revert`; its `best_height` is the live pruning bound, lowered on
// every improvement and never restored. The best complete solution goes to
// `best`. `shared` (when non-null) is the portfolio-wide atomic bound; the
// decompose path merges sub-part solutions through `solve_decomposed`.
void solve_dfs(Partition& node, int64_t min_offset, int min_idx, SearchCtx& ctx,
               std::optional<Solution>& best, std::atomic<int64_t>* shared) {
  // A sub-part only has to fit the inherited bound, not reach its own optimum
  const bool first_leaf = shared == nullptr;
  ++ctx.nodes;

  if (node.is_allocated()) {
    const int64_t h = node.height();
    if (h < node.best_height()) {
      best = Solution{node.allocations(), node.offsets(), h};
      node.set_best_height(h);
      lower_shared(shared, h);
    }
    return;
  }
  // The clock is orders of magnitude costlier than a node, so sample it every
  // 256 nodes; the deadline loses at most microseconds of precision.
  if (ctx.nodes >= ctx.node_limit ||
      ctx.portfolio_best->load(std::memory_order_relaxed) <=
          ctx.problem_lower_bound ||
      ((ctx.nodes & 255) == 0 &&
       std::chrono::steady_clock::now() > ctx.deadline)) {
    ctx.stopped = true;
    return;
  }

  pull_shared(shared, node);

  if (ctx.opts.decompose) {
    if (std::optional<std::vector<Partition>> sub_parts = node.decompose()) {
      solve_decomposed(*sub_parts, node, ctx, best, shared);
      return;
    }
  }

  const std::set<std::pair<int64_t, int>>& candidates = node.candidates();
  const std::vector<int>& sym_pred = node.sym_predecessor();
  const std::vector<int64_t>& offsets = node.offsets();
  const int64_t min_height = node.min_height();
  // Invariants across the loop: `revert` restores floors/totals before each
  // iteration's break check.
  const auto [lower_bound, max_total] = node.scan_bounds();

  // Skip the candidates that lex-precede the canonical (min_offset, idx)
  // floor. `apply_at`/`revert` mutate the set, so re-seek the iterator after
  // each try; the net state is unchanged, making the saved key a valid anchor.
  auto it = ctx.opts.canonical ? candidates.lower_bound({min_offset, min_idx})
                               : candidates.begin();
  while (it != candidates.end()) {
    // Nothing below `lower_bound` exists in this subtree, so once the bound
    // reaches it (via a child or a sibling portfolio thread), stop; checking
    // before each candidate lets an obsolete search unwind promptly.
    if (node.best_height() <= lower_bound) break;

    const auto [alloc_offset, alloc_idx] = *it;

    // Symmetry breaking: interchangeable buffers are forced into index order,
    // so skip one whose earlier-indexed twin is still unplaced. Gated on
    // canonical, of which it is a strengthening.
    if (ctx.opts.canonical) {
      const int pred = sym_pred[alloc_idx];
      if (pred >= 0 && offsets[pred] < 0) {
        ++it;
        continue;
      }
    }

    // Dominance: every later candidate sits at or above the lowest unplaced
    // buffer's top, so it is dominated too.
    if (ctx.opts.dominance && alloc_offset >= min_height) break;

    if (!node.can_allocate_at(alloc_idx, ctx.opts.monotonic_floor, lower_bound,
                              max_total)) {
      ++it;
      continue;
    }

    const auto& undo = node.apply_at(alloc_idx, ctx.opts.floor_inference);
    if (node.placement_feasible(undo, alloc_offset, ctx.opts.monotonic_floor)) {
      solve_dfs(node, alloc_offset, alloc_idx, ctx, best, shared);
    }
    node.revert(undo);
    if (ctx.stopped) return;

    if (first_leaf && best) return;
    it = candidates.upper_bound({alloc_offset, alloc_idx});
  }
}

// solve_dfs recurses once per placed allocation, so worker stacks must scale
// with the instance size; secondary threads get small default stacks
// (512 KiB on macOS).
constexpr size_t kWorkerStackBytes = size_t{64} << 20;

#if defined(_WIN32)

unsigned __stdcall worker_entry(void* arg) {
  (*static_cast<std::function<void()>*>(arg))();
  return 0;
}

// Run `worker` on `count` threads with `kWorkerStackBytes` stacks; join all.
void run_worker_threads(std::function<void()>& worker, int count) {
  std::vector<HANDLE> threads;
  threads.reserve(static_cast<size_t>(count));
  for (int t = 0; t < count; ++t) {
    const uintptr_t handle = _beginthreadex(
        nullptr, static_cast<unsigned>(kWorkerStackBytes), worker_entry,
        &worker, STACK_SIZE_PARAM_IS_A_RESERVATION, nullptr);
    if (handle == 0) break;  // join what was spawned; work is still completed
    threads.push_back(reinterpret_cast<HANDLE>(handle));
  }
  if (threads.empty()) {
    worker();
    return;
  }
  for (HANDLE t : threads) {
    WaitForSingleObject(t, INFINITE);
    CloseHandle(t);
  }
}

#else

void* worker_entry(void* arg) {
  (*static_cast<std::function<void()>*>(arg))();
  return nullptr;
}

// Run `worker` on `count` threads with `kWorkerStackBytes` stacks; join all.
void run_worker_threads(std::function<void()>& worker, int count) {
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, kWorkerStackBytes);

  std::vector<pthread_t> threads;
  threads.reserve(static_cast<size_t>(count));
  for (int t = 0; t < count; ++t) {
    pthread_t thread{};
    if (pthread_create(&thread, &attr, worker_entry, &worker) != 0) {
      break;  // join what was spawned; work is still completed
    }
    threads.push_back(thread);
  }
  pthread_attr_destroy(&attr);

  if (threads.empty()) {
    worker();
    return;
  }
  for (pthread_t t : threads) pthread_join(t, nullptr);
}

#endif

// Run `work` on `num_threads` sized-stack threads (clamped to [1, num_tasks])
// and join them; even a single worker gets its own thread so the deep
// `solve_dfs` recursion never runs on the caller's default stack. The first
// exception any worker raised is rethrown on the calling thread — letting it
// escape a thread entry point would call std::terminate.
void run_workers(const std::function<void()>& work, int num_threads,
                 size_t num_tasks) {
  std::mutex error_mutex;
  std::exception_ptr error;
  std::function<void()> worker = [&]() noexcept {
    try {
      work();
    } catch (...) {
      const std::lock_guard<std::mutex> lock(error_mutex);
      if (!error) error = std::current_exception();
    }
  };

  const int count = std::clamp(
      num_threads, 1, static_cast<int>(std::max<size_t>(num_tasks, 1)));
  run_worker_threads(worker, count);
  if (error) std::rethrow_exception(error);
}

// Unlike the allocators, `greedy_many`/`solve_many` treat a non-positive
// timeout as an already-expired budget rather than a disabled one, so a
// missing deadline collapses to "now".
std::chrono::steady_clock::time_point compute_deadline(double timeout) {
  return make_deadline(timeout).value_or(std::chrono::steady_clock::now());
}

}  // namespace

Solution greedy_many(const Partition& partition,
                     const std::vector<std::string>& heuristics, double timeout,
                     int num_threads) {
  if (heuristics.empty()) {
    throw std::invalid_argument("greedy_many requires at least one heuristic");
  }
  validate_heuristics(heuristics);

  const auto deadline = compute_deadline(timeout);
  std::vector<std::optional<Solution>> results(heuristics.size());
  std::atomic<size_t> next{0};

  // The first heuristic is packed regardless of the deadline so that at
  // least one result always exists.
  run_workers(
      [&]() {
        for (size_t i = next.fetch_add(1); i < heuristics.size();
             i = next.fetch_add(1)) {
          if (i > 0 && std::chrono::steady_clock::now() > deadline) break;
          results[i] = partition.greedy_pack(heuristics[i]);
        }
      },
      num_threads, heuristics.size());

  std::optional<Solution> best;
  for (auto& r : results) {
    if (r && (!best || r->height < best->height)) best = std::move(*r);
  }
  return std::move(*best);
}

std::optional<Solution> solve_many(const std::vector<Partition>& partitions,
                                   int64_t node_limit, double timeout,
                                   int64_t best_bound, SearchOptions options,
                                   int num_threads) {
  if (partitions.empty()) return std::nullopt;

  const auto deadline = compute_deadline(timeout);
  std::atomic<int64_t> shared_best{best_bound};
  std::vector<std::optional<Solution>> results(partitions.size());
  std::atomic<size_t> next{0};

  run_workers(
      [&]() {
        for (size_t i = next.fetch_add(1); i < partitions.size();
             i = next.fetch_add(1)) {
          Partition root = partitions[i];
          root.set_best_height(std::min(
              root.best_height(), shared_best.load(std::memory_order_relaxed)));
          SearchCtx ctx{node_limit, deadline, options, &shared_best,
                        root.lower_bound()};
          solve_dfs(root, 0, 0, ctx, results[i], &shared_best);
        }
      },
      num_threads, partitions.size());

  std::optional<Solution> best;
  for (auto& r : results) {
    if (r && (!best || r->height < best->height)) best = std::move(*r);
  }
  return best;
}

}  // namespace omnimalloc
