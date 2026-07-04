//
// SPDX-License-Identifier: Apache-2.0
//

#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/unordered_map.h>
#include <nanobind/stl/unordered_set.h>
#include <nanobind/stl/variant.h>
#include <nanobind/stl/vector.h>

#include <sstream>

#include "allocators/greedy.hpp"
#include "allocators/greedy_base.hpp"
#include "allocators/supermalloc/partition.hpp"
#include "primitives/allocation.hpp"
#include "primitives/buffer_kind.hpp"
#include "primitives/id_type.hpp"

namespace nb = nanobind;
using namespace nb::literals;
using namespace omnimalloc;

NB_MODULE(_cpp, m) {
  // BufferKind enum
  nb::enum_<BufferKind>(m, "BufferKind")
      .value("WORKSPACE", BufferKind::WORKSPACE)
      .value("CONSTANT", BufferKind::CONSTANT)
      .value("INPUT", BufferKind::INPUT)
      .value("OUTPUT", BufferKind::OUTPUT)
      .def_prop_ro("is_io", [](BufferKind kind) { return is_io(kind); })
      .def("__str__",
           [](BufferKind kind) {
             std::ostringstream ss;
             ss << kind;
             return ss.str();
           })
      .def("__repr__",
           [](BufferKind kind) {
             std::ostringstream ss;
             ss << kind;
             return ss.str();
           })
      .def("__hash__", std::hash<BufferKind>{});

  // Allocation class
  nb::class_<Allocation>(m, "Allocation")
      .def(nb::init<IdType, int64_t, int64_t, int64_t, std::optional<int64_t>,
                    std::optional<BufferKind>>(),
           "id"_a, "size"_a, "start"_a, "end"_a, "offset"_a = nb::none(),
           "kind"_a = nb::none())
      .def_prop_ro("id", &Allocation::id, nb::rv_policy::copy)
      .def_prop_ro("size", &Allocation::size)
      .def_prop_ro("start", &Allocation::start)
      .def_prop_ro("end", &Allocation::end)
      .def_prop_ro("offset", &Allocation::offset, nb::rv_policy::copy)
      .def_prop_ro("kind", &Allocation::kind, nb::rv_policy::copy)
      .def_prop_ro("is_allocated", &Allocation::is_allocated)
      .def_prop_ro("duration", &Allocation::duration)
      .def_prop_ro("height", &Allocation::height, nb::rv_policy::copy)
      .def_prop_ro("area", &Allocation::area)
      .def("overlaps_temporally", &Allocation::overlaps_temporally, "other"_a)
      .def("overlaps_spatially", &Allocation::overlaps_spatially, "other"_a)
      .def("overlaps", &Allocation::overlaps, "other"_a)
      .def("with_offset", &Allocation::with_offset, "offset"_a,
           nb::rv_policy::move)
      .def("with_kind", &Allocation::with_kind, "kind"_a, nb::rv_policy::move)
      .def("__str__",
           [](const Allocation& a) {
             std::ostringstream ss;
             ss << a;
             return ss.str();
           })
      .def("__repr__",
           [](const Allocation& a) {
             std::ostringstream ss;
             ss << a;
             return ss.str();
           })
      .def("__eq__", &Allocation::operator==)
      .def("__hash__", std::hash<Allocation>{})
      .def("__getstate__",
           [](const Allocation& a) {
             return std::make_tuple(a.id(), a.size(), a.start(), a.end(),
                                    a.offset(), a.kind());
           })
      .def("__setstate__",
           [](Allocation& a,
              const std::tuple<IdType, int64_t, int64_t, int64_t,
                               std::optional<int64_t>,
                               std::optional<BufferKind>>& state) {
             new (&a) Allocation(std::get<0>(state), std::get<1>(state),
                                 std::get<2>(state), std::get<3>(state),
                                 std::get<4>(state), std::get<5>(state));
           });

  m.def("compute_temporal_overlaps", &compute_temporal_overlaps,
        "allocations"_a, nb::rv_policy::move);
  m.def("first_fit_place", &first_fit_place, "allocations"_a, "overlaps"_a,
        nb::rv_policy::move);

  // FirstFitPlacer class: resident placer for the order-search allocators
  nb::class_<FirstFitPlacer>(m, "FirstFitPlacer")
      .def(nb::init<std::vector<Allocation>>(), "allocations"_a)
      .def("evaluate", &FirstFitPlacer::evaluate, "order"_a)
      .def("place", &FirstFitPlacer::place, "order"_a, nb::rv_policy::move)
      .def_prop_ro("overlaps", &FirstFitPlacer::overlaps, nb::rv_policy::copy);

  // GreedyAllocator class
  nb::class_<GreedyAllocator>(m, "GreedyAllocatorCpp")
      .def(nb::init<>())
      .def("allocate", &GreedyAllocator::allocate, "allocations"_a,
           nb::rv_policy::move)
      .def("__str__",
           [](const GreedyAllocator&) { return "GreedyAllocator()"; })
      .def("__repr__",
           [](const GreedyAllocator&) { return "GreedyAllocator()"; })
      .def("__eq__", &GreedyAllocator::operator==)
      .def("__hash__", std::hash<GreedyAllocator>{});

  // SearchOptions class
  constexpr SearchOptions kDefaultOptions{};
  nb::class_<SearchOptions>(m, "SearchOptions")
      .def(nb::init<bool, bool, bool, bool, bool>(),
           "canonical"_a = kDefaultOptions.canonical,
           "dominance"_a = kDefaultOptions.dominance,
           "floor_inference"_a = kDefaultOptions.floor_inference,
           "monotonic_floor"_a = kDefaultOptions.monotonic_floor,
           "decompose"_a = kDefaultOptions.decompose)
      .def_rw("canonical", &SearchOptions::canonical)
      .def_rw("dominance", &SearchOptions::dominance)
      .def_rw("floor_inference", &SearchOptions::floor_inference)
      .def_rw("monotonic_floor", &SearchOptions::monotonic_floor)
      .def_rw("decompose", &SearchOptions::decompose);

  // Partition class
  nb::class_<Partition>(m, "Partition")
      .def_static("from_allocations", &Partition::from_allocations,
                  "allocations"_a, nb::rv_policy::move)
      .def("greedy_pack", &Partition::greedy_pack, "heuristic"_a,
           nb::rv_policy::move)
      .def("reorder", &Partition::reorder, "heuristic"_a, nb::rv_policy::move)
      .def("with_bound", &Partition::with_bound, "bound"_a, nb::rv_policy::move)
      .def_prop_ro("lower_bound", &Partition::lower_bound);

  // Solution class
  nb::class_<Solution>(m, "Solution")
      .def_ro("allocations", &Solution::allocations)
      .def_ro("offsets", &Solution::offsets)
      .def_ro("height", &Solution::height);

  m.def("greedy_many", &greedy_many, "partition"_a, "heuristics"_a,
        "max_seconds"_a, "num_threads"_a,
        nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);

  m.def("solve_many", &solve_many, "partitions"_a, "node_limit"_a,
        "max_seconds"_a, "best_bound"_a, "options"_a, "num_threads"_a,
        nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);
}
